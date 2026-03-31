from typing import List, Dict, Any

class HistoryManager:
    def __init__(self):
        # 1. 대화 내용 저장소 (LLM 문맥용)
        self.chat_storage: Dict[str, List[str]] = {}
        
        # 2. 추출된 정보 상태 저장소 (슬롯 필링용)
        # 구조: { "session_id": { "location": "수원시 영통구", "property_type": "아파트", ... } }
        self.state_storage: Dict[str, Dict[str, Any]] = {}

    # --- 대화 기록 관련 ---
    def add_user_message(self, session_id: str, message: str):
        if session_id not in self.chat_storage:
            self.chat_storage[session_id] = []
        self.chat_storage[session_id].append(f"User: {message}")

    def add_system_message(self, session_id: str, message: str):
        if session_id not in self.chat_storage:
            self.chat_storage[session_id] = []
        self.chat_storage[session_id].append(f"System: {message}")

    def get_context_string(self, session_id: str) -> str:
        if session_id not in self.chat_storage:
            return ""
        return "\n".join(self.chat_storage[session_id][-4:])

    # --- 상태(State) 관리 관련 ---
    def get_state(self, session_id: str) -> dict:
        """현재까지 모인 정보를 가져온다."""
        return self.state_storage.get(session_id, {})

    def update_state(self, session_id: str, new_fields: dict) -> dict:
        """
        새로 추출된 정보를 기존 정보와 합친다 (Merge).
        새로운 값이 존재할 경우 기존 값을 덮어쓴다.
        """
        if session_id not in self.state_storage:
            self.state_storage[session_id] = {
                "main_intent": None, "location": None, "complex_name": None,
                "property_type": None, "price_metric": None, 
                "start_date": None, "end_date": None
            }
        
        current_state = self.state_storage[session_id]

        for key, value in new_fields.items():
            if value is not None and value != "":
                current_state[key] = value
        
        self.state_storage[session_id] = current_state
        return current_state

# 전역 인스턴스
history_manager = HistoryManager()