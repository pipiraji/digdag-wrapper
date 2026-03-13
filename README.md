# digdag-wrapper
서버 자동 기동 및 워크플로우 실행 최적화 기능을 갖춘 Digdag CLI 확장 래퍼 스크립트.


💡 What is Digdag?
**Digdag**은 복잡한 데이터 파이프라인과 워크플로우를 단순하게 관리할 수 있도록 돕는 오픈소스 워크플로우 엔진입니다. Treasure Data에서 개발하였으며, 다음과 같은 강력한 특징을 가지고 있습니다.

YAML 기반 정의: 복잡한 코딩 없이 .dig 설정 파일(YAML)만으로 태스크 간의 의존성(DAG)을 정의합니다.

다양한 언어 지원: Shell, Python, SQL, Ruby 등 다양한 환경의 스크립트를 하나의 워크플로우 안에서 유기적으로 실행할 수 있습니다.

서버 & 로컬 모드: 로컬에서 간단히 테스트하거나, 서버 모드로 띄워 스케줄링 및 UI를 통한 모니터링이 가능합니다.

강력한 오류 제어: 실패한 태스크의 재시도(Retry), 에러 통보, 파라미터 전달 등을 유연하게 처리합니다.

🚩 Why this Wrapper?
Digdag은 매우 훌륭한 도구이지만, 실무 환경(특히 멀티 유저가 사용하는 컴퓨팅 팜이나 서버)에서 사용할 때는 몇 가지 번거로운 점이 있습니다.

서버 관리의 불편함: 매번 서버를 수동으로 띄우고 포트를 확인해야 합니다.

경쟁 상태(Race Condition): 여러 사용자가 같은 서버 자원을 사용할 때 충돌이 발생할 수 있습니다.

반복되는 명령어: push, start, check 등의 명령어를 매번 따로 입력해야 합니다.

본 프로젝트인 **digdag-wrapper**는 이러한 불편함을 해소하고, 서버 기동부터 워크플로우 실행까지 단 한 줄의 명령어로 자동화하여 작업 효율을 극대화합니다.

🛠 Key Features of this Wrapper
Auto-Server Lifecycle: 서버가 없으면 자동으로 빈 포트를 찾아 기동하고, 필요 시 종료합니다.

Race Condition Protection: Lock 메커니즘을 통해 하나의 계정에서 중복된 서버가 실행되는 것을 방지합니다.

Smart Subcommands: run_workflow, list_job, kill_job 등 기존 CLI보다 직관적인 명령어를 제공합니다.

Environment Optimized: 고정된 임시 디렉토리 관리로 멀티 노드 환경에서도 안정적인 실행을 보장합니다.


커맨드 목록
커맨드동작
digdag start_server서버 기동
digdag kill_server서버 종료
digdag run_workflow <project> <workflow>서버 기동 + push + start
digdag list_jobattempts 목록 조회
digdag kill_jobattempts kill
digdag browseUI 열기



