from main import QBrainReasoningEngine

def main():
    engine = QBrainReasoningEngine()

    # Important: set_up() is NOT auto-called locally
    engine.set_up()

    result = engine.query({
        "query": "what do you know about  tickets sold",
        "sessionid": "683472370744164352",
        "userid": "nvalappil"
    })

    print(result)

if __name__ == "__main__":
    main()
