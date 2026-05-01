import os
import pandas as pd
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from datasets import Dataset
import dotenv
# ------------------------------------------------------------------------------
# 1. RAGAS 4대 핵심 지표 및 임베딩 설정
# ------------------------------------------------------------------------------
from ragas import evaluate
from ragas.metrics import (
    faithfulness,        
    answer_relevancy,    
    context_recall,      
    context_precision    
)
from langchain_openai import OpenAIEmbeddings  # 임베딩 모델 추가

# ------------------------------------------------------------------------------
# 2. [필수 설정] API 키 및 임베딩 모델 정의
# ------------------------------------------------------------------------------
dotenv.load_dotenv()
OPENAI_API_KEY =  os.getenv('Emb_KEY')

# answer_relevancy 계산을 위해 임베딩 모델을 정의합니다.
judge_llm = ChatOpenAI(model="gpt-4o", temperature=0)
embeddings_model = OpenAIEmbeddings(model="text-embedding-3-small")

# ------------------------------------------------------------------------------
# 3. 데이터 로드 및 데이터셋 준비 (수정된 시나리오 반영)
# ------------------------------------------------------------------------------
data_samples = {
    # 시나리오 질문
    "user_input": [
        # 시나리오 1: 
        "수원시 영통구 매탄동에 있는 매탄레이크파크 아파트 10층, 2026년 1월 29일에 거래된 거 가격 얼마야?",
        # 시나리오 2: 
        "2026년 1월 28일에 거래된 수원시 영통구 원천동 광교아이파크 34층 매물 말이야, 13억 5천에 거래된 거 정상적으로 계약 유지 중인 매물이야?",
        # 시나리오 3:
        "올해 1월 초(1일~5일 사이)에 거래된 부천시 원미구 상동에 있는 상동스카이뷰자이 매매 시세 좀 알려줘."
    ],
    
    "retrieved_contexts": [
        # 검색된 contexts 
        [
            " "
        ],
        
        
        [
            " "
        ],
        
        
        [
            " "
        ]
    ],
    
    "response": [
        # AI의 최종 답변
        " ",

        " ",

        " "
    ],
    # 시나리오 답변
    "reference": [
        # 시나리오 1:
        "2026년 1월 29일 거래된 수원시 영통구 매탄동 '매탄레이크파크' 아파트 10층의 실거래 가격은 3억 9,500만 원입니다. 해당 건은 정상적으로 중개 거래된 매물입니다.",
        # 시나리오 2:
        "문의하신 2026년 1월 28일 자 원천동 '광교아이파크' 34층 매물은 13억 5,000만 원에 계약되었으나, 2026년 2월 25일부로 거래가 해제되었습니다. 현재 정상 계약 유지 매물이 아닙니다.",
        # 시나리오 3:
        "문의하신 기간(2026년 1월 1일 ~ 1월 5일) 내 부천시 원미구 상동 '상동스카이뷰자이' 매매 내역을 조회한 결과, 1월 3일에 7억 7,000만 원(18층)으로 거래된 내역이 존재합니다. 단, 해당 거래는 2026년 1월 16일부로 계약이 해제된 상태이므로 시세 파악 시 주의가 필요합니다."
    ]
}

dataset = Dataset.from_dict(data_samples)

# ------------------------------------------------------------------------------
# 4. RAGAS 평가 실행 (embeddings 파라미터 추가)
# ------------------------------------------------------------------------------
print(" [System] RAGAS 4대 지표 평가를 시작합니다...")

result = evaluate(
    dataset=dataset,
    metrics=[
        faithfulness,
        answer_relevancy,
        context_recall,
        context_precision
    ],
    llm=judge_llm,
    embeddings=embeddings_model  # answer_relevancy 측정을 위해 필수 추가
)

# ------------------------------------------------------------------------------
# 5. 결과 처리 및 출력
# ------------------------------------------------------------------------------
df_result = result.to_pandas()
df_result['hallucination_rate(%)'] = (1.0 - df_result['faithfulness'].fillna(0)) * 100

print("\n [세부 채점 결과]")
print(df_result[['user_input', 'context_recall', 'faithfulness', 'answer_relevancy', 'hallucination_rate(%)']])

# 엑셀 파일로 저장
df_result.to_csv("result_KCBERT_Llama.csv", index=False, encoding='utf-8-sig')