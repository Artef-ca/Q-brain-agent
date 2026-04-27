import os
from vertexai.preview.reasoning_engines import ReasoningEngine

# Deploys the agent code to Agent Engine runtime by pointing to a source dir that contains:
# main.py, requirements.txt, and your code package.
# This pattern is documented in ReasoningEngine class reference. :contentReference[oaicite:9]{index=9}

PROJECT_ID = os.environ["PROJECT_ID","smartadvisor-483817"]
REGION = os.environ.get("REGION","us-central1")

if __name__ == "__main__":
    # This assumes you prepare a 'user_src_dir' layout as described in the Vertex docs
    # (You can also keep this repo as the src_dir if it matches the expectation).
    src_dir = os.path.abspath(".")
    engine = ReasoningEngine.create(
        display_name="qbrain-root-agent",
        src_dir=src_dir,
        requirements=["-requirements.txt"],
    )
    print("Deployed:", engine.resource_name)
