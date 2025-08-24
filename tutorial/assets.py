from dagster import AssetSelection,  asset , AssetExecutionContext , AssetIn , Definitions , define_asset_job , ScheduleDefinition, op, graph, OpExecutionContext 

@asset(group_name="AWSOM_group")
def first_asset(context: AssetExecutionContext):
    """THis is first Asset"""
    context.log.info("Hello World 1")
    metadata = {
        "name": "AWSOM_first_asset",
        "description": "This is first asset",
        "owner": "AWSOM",
        "tags": ["AWSOM_tag"]
    }
    context.add_output_metadata(metadata)
    return "Hello World"

# way One for dependency 
# @asset
# def second_asset(context: AssetExecutionContext,first_asset:str):
#     """This is second Asset"""
#     context.log.info(f"Hello World 2 ######## {first_asset}")
#     return "Hello World 2"


@asset(ins={"dep1":AssetIn("first_asset")},group_name="AWSOM_group")
def second_asset(context: AssetExecutionContext,dep1:str):
    """This is second Asset"""
    metadata = {
        "name": "AWSOM_second_asset",
        "description": "This is second asset",
        "owner": "AWSOM",
        "tags": ["AWSOM_tag"]
    }
    context.log.info(f"Hello World 2 ######## {dep1}")
    context.add_output_metadata(metadata)
    return "Hello World 2"

# way 2 is just use deps=[]

#way 3 using ins
@asset(ins={
 "dep1":AssetIn("first_asset")
} , key = "AWSOM_third_asset" , group_name="AWSOM_group")
def third_asset(context: AssetExecutionContext,dep1:str):
    """This is third Asset"""
    metadata = {
        "name": "AWSOM_third_asset",
        "description": "This is third asset",
        "owner": "AWSOM",
        "tags": ["AWSOM_tag"]
    }
    context.add_output_metadata(metadata)
    context.log.info(f"Hello World 3 ######## {dep1}")
    return "Hello World 3"

# way 4 using ins
@asset(ins={"dep3":AssetIn("AWSOM_third_asset")},group_name="AWSOM_group")
def fourth_asset(context: AssetExecutionContext,dep3:str):
    """This is fourth Asset"""
    metadata = {
        "name": "AWSOM_fourth_asset",
        "description": "This is fourth asset",
        "owner": "AWSOM",
        "tags": ["AWSOM_tag"]
    }
    context.add_output_metadata(metadata)
    context.log.info(f"Hello World 4 ######## {dep3}")
    return "Hello World 4"

# ===== OPS WITH DEPENDENCIES =====
@op
def step_one_op(context: OpExecutionContext) -> str:
    """First step - like your first_asset"""
    context.log.info("Step 1: Starting process")
    return "Step 1 completed"

@op  
def step_two_op(context: OpExecutionContext, step1_result: str) -> str:
    """Second step - depends on step_one_op"""
    context.log.info(f"Step 2: Using result from step 1: {step1_result}")
    return f"Step 2 completed using: {step1_result}"

@op
def step_three_op(context: OpExecutionContext, step1_result: str, step2_result: str) -> str:
    """Third step - depends on both previous steps"""
    context.log.info(f"Step 3: Using {step1_result} and {step2_result}")
    return f"Final result: {step1_result} + {step2_result}"

@graph
def op_workflow():
    step1 = step_one_op()
    step2 = step_two_op(step1)
    step3 = step_three_op(step1, step2)
    return step3


# partition_def = DailyPartitionsDefinition(start_date="2025-01-01")


# @asset(partitions_def=partition_def , retry_policy=RetryPolicy(max_retries=3 , delay=5))
# def partition_asset(context: AssetExecutionContext):
#     """This is partition asset"""
#     return "Hello World"

op_job = op_workflow.to_job(name="op_workflow_job")

job = define_asset_job(
    name="AWSOM_job" , 
   # selection=["first_asset" , "second_asset" , "AWSOM_third_asset"]
   selection= AssetSelection.groups("AWSOM_group")
    )


schedule = ScheduleDefinition(
    name="AWSOM_schedule",
    job=job,
    execution_timezone="Asia/Kolkata",
    cron_schedule="* * * * *"
)

schedule_op = ScheduleDefinition(
    name="AWSOM_schedule_op",
    job=op_job,
    execution_timezone="Asia/Kolkata",
    cron_schedule="* * * * *"
)

defs = Definitions(
    assets=[first_asset,second_asset,third_asset,fourth_asset],
    jobs=[job,op_job],
    schedules=[schedule,schedule_op]
    )
