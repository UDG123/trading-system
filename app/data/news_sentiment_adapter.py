class NewsSentimentAdapter:
    def analyze(self,query:str):
        return {'query':query,'sentiment':'neutral','score':0.0}
