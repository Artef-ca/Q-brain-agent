import vertexai
from vertexai.preview import reasoning_engines

# 1. Initialize Vertex AI
vertexai.init(
    project="prj-ai-dev-qic", 
    location="europe-west1"
)

# 2. Connect to your deployed Reasoning Engine
# agent = reasoning_engines.ReasoningEngine("231569243437531136")
agent = reasoning_engines.ReasoningEngine("3614077128313667584")


# 3. Query the agent using the exact parameter name "request"
print("Sending query to agent...")
response = agent.query(
    request={
        "query": "give me top 5 rides with highest queue time",
        "sessionid": "htr9",
        "userid": "u1"
    }
)

# 4. Print the result
print("\n--- Agent Response ---")
print(response)


# projects/960407631274/locations/us-central1/reasoningEngines/8772364461768966144

# import vertexai
# from vertexai.preview import reasoning_engines

# # 1. Initialize Vertex AI with your project details
# vertexai.init(
#     project="smartadvisor-483817", 
#     location="us-central1"
# )

# # 2. Connect to your deployed Reasoning Engine
# agent = reasoning_engines.ReasoningEngine("231569243437531136")

# # 3. Query the agent
# print("Sending query to agent...")
# response = agent.query(

#         "question": "hello",
#         "session_id": "s1",
#         "user_id": "u1"

# )
# # 4. Print the result
# print("\n--- Agent Response ---")
# print(response)


# import vertexai
# from vertexai.preview.reasoning_engines import ReasoningEngine
# from google.cloud import aiplatform_v1


# PROJECT_ID = "smartadvisor-483817"
# REGION = "us-central1"
# ENGINE_ID = '231569243437531136'

# RESOURCE_NAME = "projects/960407631274/locations/us-central1/reasoningEngines/231569243437531136"

# vertexai.init(project=PROJECT_ID, location=REGION)

# # engine = ReasoningEngine.get(resource_name=RESOURCE_NAME)

# client = aiplatform_v1.ReasoningEngineExecutionServiceClient(
#     client_options={"api_endpoint": f"{REGION}-aiplatform.googleapis.com"}
# )

# endpoint = f"projects/{PROJECT_ID}/locations/{REGION}/reasoningEngines/{ENGINE_ID}"

# response = client.query_reasoning_engine(
#     name=endpoint,
#     input={
#         "question": "Hello",
#         "session_id": "test_session_1",
#         "user_id": "test_user_1"
#     }
# )

# print(response) 

# # https://us-central1-aiplatform.googleapis.com/v1/projects/smartadvisor-483817/locations/us-central1/reasoningEngines/231569243437531136:query
