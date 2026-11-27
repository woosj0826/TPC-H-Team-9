# 재현 안내

이 패키지의 Spark 작업은 실험 당시 코드를 보존한 것이다. 신규 클러스터에서는 스크립트의 ROOT, HDFS URI, 드라이버/워커 주소를 새 환경에 맞춘 뒤 실행한다. 전용 HDFS 경로와 Hive 카탈로그를 사용하며 기존 데이터베이스와 충돌하지 않는지 확인한다.

1. Java 11, Spark 3.5.7 및 Hadoop 3.3.6 HDFS/YARN을 설치한다. HDFS 복제 2와 블록 크기 128MiB, NodeManager 8 vcore와 65,536MiB를 설정한다. Spark 설정은 [예시](../conf/spark-defaults.conf.example)를 사용하고, YARN과 Hive 접속 설정을 추가한다.
2. TPC-H dbgen으로 SF별 8개 `.tbl` 파일을 생성한다. 입력 URI 및 manifest 형식은 각 준비 스크립트가 정의한다. SF1·10·30은 `prepare-sf1.py`, `prepare.py`, `prepare-sf30.py`로 기본 레이아웃을 만든 뒤 `prepare-optimized.py --sf N`을 실행한다. SF100은 `build-sf100-direct.py`가 지정한 로컬 입력과 manifest를 준비하고, 저장 데이터에 접근 가능한 Spark 환경에서 실행한다.
3. SF1·10·30 전체 최적화 측정은 `spark-submit jobs/run-benchmark.py --sf N --layout optimized --aqe off --warmup 1 --repeat 3`으로 실행한다. SQL 22개와 출처는 `sql/`에 있다. 같은 애플리케이션에서 다른 측정이 겹치지 않도록 한다.
4. SF10은 `prepare-ablation-layouts.py`, SF100은 `build-sf100-ablation-layout.py`로 ablation 레이아웃을 준비한다. `benchmark-ablation.py` 또는 `benchmark-ablation-sf100.py`에서 `--sf`, `--case`, `--output`, `--reference`를 지정한다. 기준 결과의 형식과 조건별 설정은 스크립트 및 `ablation_config.py`에 있다.
5. SF100 노드 확장은 `reference/`의 등록용 JSON 3개를 해당 실행 루트의 `results/`에 배치한다. DDL의 데이터 경로를 새 HDFS URI에 맞추고 `scaling_register.py`로 테이블과 통계를 등록한다. 활성 DataNode와 YARN 워커가 동일한지, 블록당 복제본 2개와 워커 사이의 TCP 연결이 정상인지 검증한다. 각 구성에서 `spark-submit --num-executors N jobs/scaling_benchmark.py --nodes N --hosts HOST1,HOST2,... --output OUTPUT`을 실행한다.

공유 저장소에는 운영 서버 접속용 자격 증명, 원시 데이터, 전체 Spark/YARN 로그를 포함하지 않았다. 이 패키지는 원본 작업과 재현에 필요한 예시를 제공한다. 새로운 클러스터에서 전체 실행은 검증하지 않았다.
