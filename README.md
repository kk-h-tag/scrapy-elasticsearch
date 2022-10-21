## Project Title
- Scrapy-Elasticsearch

## Project Abstract
- Scrapy-Redis에서 Redis의 역할을 Elasticsearch로 변경하였습니다.
- 작은 자원을 가진 머신에서 Redis의 메모리 사용량을 버티지 못해 검색엔진을 사용하였습니다.
- 여러개의 Spider를 실행해 하나의 대기열(HOST)를 공유하여 중복을 방지해 크롤링을 진행할 수 있습니다.

## Requirement
- scrapy >= 2.0
- Elasticsearch <= 7.0
- elasticsearch <= 본인의 Elasticsearch 버전

## 실행방법
- Git에서 코드를 clone.
- clone한 코드를 본인의 scrapy 프로젝트에 복사.
- Scrapy-Elasticsearch에 connection의 get_els 함수에 본인의 Elasticsearch IP 혹은 Domain을 입력.
- Scrapy settings.py를 다음과 같이 변경
```c
UPEFILTER_CLASS = "Your Scrapy Project Name.Scrapy_Elasticsearch.dupFilter.RFPDupeFilter"
SCHEDULER = "Your Scrapy Project Name.Scrapy_Elasticsearch.scheduler.Scheduler"
SCHEDULER_QUEUE_CLASS = "Your Scrapy Project Name.Scrapy_Elasticsearch.queue.SpiderQueue"
```
- 실행 전 spider가 바라볼 INDEX에 startUrl이 있어야 함.
- 반드시 scrapy를 실행 할 시 해당 spider가 중복검사와 Request를 꺼내올 INDEX를 setting에서 지정해주거나 인자로 넣어줘야함.
```c
scrapy crawl YOUR BOT NAEM -s ELS_INDEX=$index
```
- 검색엔진의 index 구성은 아래 mapping을 확인하세요.


## 검색엔진의 mappings
```c
"mappings": {
  "properties": {
    "type": { "type": "keyword"},
    "url": { "type": "keyword"}
  }
}
```

## Todo List
- Elasticsearch 이외의 검색엔진에서도 작동 여부 확인.

##Credit
- [Scrapy-Redis](https://github.com/rmax/scrapy-redis) 코드의 Redis 부분을 Elastiscsearch로 변경하여 개발하였습니다.
