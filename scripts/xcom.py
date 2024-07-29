def use_s3_path_from_xcom(task_ids, **kwargs):
    task_instance = kwargs['task_instance']
    print(f"task_instance: {task_instance}")
    s3_path = task_instance.xcom_pull(task_ids=task_ids, key='s3_path')
    if not s3_path:
        raise ValueError(f"No S3 path found for task ID {task_ids}")
    # Your logic to use the S3 path
    print(f"Using S3 path: {s3_path}")

    return s3_path

# def use_s3_path_from_xcom(**kwargs):
#     task_instance = kwargs['ti']
#     s3_path = task_instance.xcom_pull(task_ids=kwargs['task_ids'], key='s3_path')
    
#     # Use the S3 path in your function logic
#     print(f"Using S3 path: {s3_path}")
#     # Add your logic to use the s3_path