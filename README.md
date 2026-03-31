## 📂 프로젝트 구조 및 파일 설명

```text
.
├── server.py              # [핵심] FastAPI 기반 검색 API 서버 (1GB RAM 최적화)
├── dbforrag.py            # [DB] PostgreSQL/pgvector 연결 및 하이브리드 검색 쿼리 관리
├── core/
│   └── embedding_loader.py # [Core] 임베딩 모델 Lazy Loading 및 메모리 관리 싱글톤
├── tests/                 # [Test] 시스템 안정성 검증을 위한 테스트 스위트
│   ├── unit/              # 단위 테스트 (리트리버 로직, 모델 로더 등)
│   └── integration/       # 통합 테스트 (API 엔드포인트 호출 검증)
├── requirements.txt       # 프로젝트 의존성 라이브러리 목록
├── pytest.ini             # 테스트 프레임워크 설정 파일
└── .gitignore             # 보안 및 불필요 파일(Key, .env, 캐시) 차단 설정
