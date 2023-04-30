def df_csv_to_s3(s3_config: dict, df, table: str, path: str) -> None:
    import boto3
    s3 = boto3.resource("s3",
        endpoint_url= s3_config["endpoint_url"],
        aws_access_key_id= s3_config["aws_access_key_id"],
        aws_secret_access_key= s3_config["aws_secret_access_key"]
        )
    print( df.info(memory_usage="deep") )
    obj = s3.Object(s3_config["bucket"], path)
    obj.put(
        Body= df.to_csv(index=False).encode(),
        Metadata= {
            "table": table,
            "columns": ",".join(df.columns)
            }
        )


def df_csv_to_s3_multipart(s3_config: dict, df, table: str, path: str) -> None:
        import boto3
        import math
        s3 = boto3.client(
            "s3",
            endpoint_url= s3_config["endpoint_url"],
            aws_access_key_id= s3_config["aws_access_key_id"],
            aws_secret_access_key= s3_config["aws_secret_access_key"]
            )
        print('Connection established')
        mpu = s3.create_multipart_upload(
            Bucket= s3_config["bucket"],
            Key= path,
            Metadata= {
                "table": table,
                "columns": ",".join(df.columns)
                }
            )
        print('Multipart upluad created')
        mpu_id = mpu["UploadId"]
        Parts = []
        part_size: int = 1000
        total = math.ceil(len(df) / part_size)
        print('Total parts: ', total)
        header_flag: bool = True
        
        try:
            for i in range(0, total):
                response = s3.upload_part(
                    Body= df[i*part_size : (i+1)*part_size].to_csv(index= False, header = header_flag).encode() , 
                    Bucket= s3_config["bucket"],
                    Key= path,
                    PartNumber= i,
                    UploadId = mpu_id
                )
                print("Part # ", i)
                header_flag = False
                Parts.append({"ETag": response["ETag"], "PartNumber": i})

            s3.complete_multipart_upload(
                Bucket= s3_config["bucket"],
                Key= path,
                MultipartUpload= {
                    "Parts": Parts
                },
                UploadId= mpu_id
                )
            print('Multipart upload completed')

        except Exception as e:
            print('Abort multipart upload due to Exception: ', e)
            s3.abort_multipart_upload(
                Bucket= s3_config["bucket"],
                Key= path,
                UploadId= mpu_id
            )


from airflow.models.baseoperator import BaseOperator
class S3CsvToPostgresOperator(BaseOperator):
    def __init__(self,
            s3_config: dict, 
            postgres_conn_id: str, 
            path: str,
            null_explicitly: bool = False,
            **kwargs
        ):
        self.s3_config = s3_config
        self.postgres_conn_id = postgres_conn_id 
        self.path = path
        self.null_explicitly = null_explicitly
        super().__init__(**kwargs)

    def execute(self, context):
        import boto3
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import time

        s3 = boto3.resource("s3",
                            endpoint_url=self.s3_config["endpoint_url"],
                            aws_access_key_id=self.s3_config["aws_access_key_id"],
                            aws_secret_access_key=self.s3_config["aws_secret_access_key"]
                            )
        hook = PostgresHook(self.postgres_conn_id)

        obj = s3.Object(self.s3_config["bucket"], self.path)
        data = obj.get()["Body"]
        table = obj.get()["Metadata"]["table"]
        columns = obj.get()["Metadata"]["columns"]
        
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(f"truncate table {table};")
        null_format = ""
        if self.null_explicitly:
            null_format = ", null 'null'"
        tm1 = time.perf_counter()
        cursor.copy_expert(f"COPY {table} ({columns}) FROM STDIN WITH (format CSV, header TRUE{null_format});", data)
        #print("Rows copied to table:", cursor.rowcount)
        conn.commit()
        tm1 = time.perf_counter() - tm1
        print("Time to COPY:", tm1)
        cursor.close()
        conn.close()
