import os
import re
from core.gateway.adapters import llama_engine

class StrategyExecutor:
    def __init__(self):
        self.prompt_dir = os.path.join("config", "prompt_templates", "preprocessing")

    def _load_prompt(self, filename, query):
        path = os.path.join(self.prompt_dir, filename)
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        return content.replace("{query}", query)

    def _parse_list(self, response: str) -> list:
        """
        Llama의 응답(문자열)을 깔끔한 파이썬 리스트로 변환합니다.
        예: "출력: 강남 시세, 서초 시세" -> ["강남 시세", "서초 시세"]
        """
        # 1. "출력:" 같은 앞머리 제거
        if ":" in response:
            response = response.split(":")[-1]
        
        # 2. 줄바꿈을 쉼표로 변환 (혹시 엔터로 구분했을 경우 대비)
        response = response.replace("\n", ",")
        
        # 3. 쉼표로 쪼개고 공백 제거
        items = [item.strip() for item in response.split(",") if item.strip()]
        
        return items

    def run_strategy(self, filename, query):
        # Llama에게 "너는 봇이야"라는 태그 없이, 그냥 텍스트 완성형으로 던집니다.
        # Few-shot(예시)이 있을 때는 이 방식이 훨씬 말을 잘 듣습니다.
        prompt = self._load_prompt(filename, query)
        return llama_engine.generate(prompt)

    def execute(self, query: str):
        # 1. 라우팅 (전략 선택)
        # 라우터는 JSON을 뱉어야 하므로 별도 처리
        router_prompt = self._load_prompt("router_prompt.txt", query)
        router_res = llama_engine.generate(router_prompt)
        
        strategy = "baseline"
        if "decomposition" in router_res: strategy = "decomposition"
        elif "hyde" in router_res: strategy = "hyde"
        elif "expansion" in router_res: strategy = "expansion"
        
        print(f"👉 전략 선택: [ {strategy.upper()} ]")

        # 2. 전략 실행
        result_queries = []

        if strategy == "baseline":
            result_queries = [query]

        elif strategy == "expansion":
            res = self.run_strategy("expansion_prompt_v1.txt", query)
            result_queries = self._parse_list(res)
            
        elif strategy == "decomposition":
            res = self.run_strategy("decomposition_prompt_v1.txt", query)
            result_queries = self._parse_list(res)
            
        elif strategy == "hyde":
            # HyDE는 리스트가 아니라 '가상 문서' 텍스트 1개임 [cite: 321]
            res = self.run_strategy("hyde_prompt_v1.txt", query)
            virtual_doc = res.replace("가상 문서:", "").strip()
            # 검색 팀에게는 [원본질문, 가상문서] 형태로 전달
            result_queries = [query, virtual_doc]

        return strategy, result_queries

strategy_executor = StrategyExecutor()