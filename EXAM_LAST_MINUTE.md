# 시험 직전 핵심 정리

지금까지 실습에서 틀렸던 부분을 중심으로 정리한 개인 복습 자료다. 공식 시험 범위 전체를 뜻하지 않는다.

## 1. 현재 원소의 자료형부터 확인

```python
rows                   # 리스트
rows[0]                # 측정소 딕셔너리
rows[0]["pm10Value"]    # 문자열: "25" 또는 "-"

valid_rdd = rdd.filter(lambda x: x["pm10Value"] != "-")
pm_rdd = valid_rdd.map(lambda x: int(x["pm10Value"]))
high_rdd = pm_rdd.filter(lambda x: x >= 20)
```

- 처음 두 lambda의 x는 딕셔너리다.
- 마지막 lambda의 x는 이미 정수다. 다시 x["pm10Value"]로 접근하지 않는다.
- 리스트는 숫자 인덱스, 딕셔너리는 키로 접근한다.

## 2. JSON과 XML 접근법

```python
# 서울 JSON
rows = data["RealtimeCityAir"]["row"]
pm = rows[0]["PM"]

# 에어코리아 JSON
rows = data["response"]["body"]["items"]
pm_text = rows[0]["pm10Value"]

# XML
root = lxml.etree.fromstring(r.content)
rows = root.findall("row")
for row in rows:
    pm = float(row.findtext("PM"))
```

- findall("row"): XML 요소의 리스트.
- findtext("PM"): 해당 요소의 텍스트 문자열.
- XML 응답에 r.json()을 적용하지 않는다.

## 3. 페이지 수집: 매번 새 응답에서 추출

```python
all_rows = []
for start in range(1, total_count + 1, page_size):
    end = min(start + page_size - 1, total_count)
    small_data = requests.get(f"{base_url}/{start}/{end}").json()
    rows = small_data["RealtimeCityAir"]["row"]
    all_rows.extend(rows)
    print(f"{start}~{end}: {len(rows)}건")
```

- 처음 요청의 data를 반복 사용하면 같은 행을 계속 추가하게 된다.
- extend에는 응답 딕셔너리 전체가 아니라 rows 리스트를 전달한다.
- XML도 페이지마다 새로 파싱한 s_root에서 s_root.findall("row")로 추출한다.

## 4. 결측값, 0건, 평균

```python
valid_rdd = rdd.filter(lambda x: x["pm10Value"] != "-")
pm_rdd = valid_rdd.map(lambda x: int(x["pm10Value"]))
valid_count = pm_rdd.count()

print("전체:", rdd.count())
print("유효:", valid_count)
print("제외:", rdd.count() - valid_count)

if valid_count > 0:
    print("합계:", pm_rdd.sum())
    print("최솟값:", pm_rdd.min())
    print("최댓값:", pm_rdd.max())
    print(f"평균: {pm_rdd.mean():.2f}")
else:
    print("합계: 0")
    print("최솟값: 계산 불가")
    print("최댓값: 계산 불가")
    print("평균: 계산 불가")
```

- "-"를 먼저 제외하고 숫자로 변환한다.
- 평균 계산 여부는 합계가 아니라 유효 건수로 판단한다.
- [0, 0]은 유효 값 2개이며 평균은 0이다.
- 빈 RDD에 reduce/min/max를 호출하지 않는다.
- 이번 페이지 제외 건수는 len(rows) - valid_count다. API 전체 totalCount에서 빼지 않는다.

## 5. RDD 함수와 반환값

| 함수 | 역할 |
|---|---|
| filter | 조건에 맞는 원소를 남긴다 |
| map | 원소 하나를 다른 원소 하나로 바꾼다 |
| flatMap | 각 원소를 변환한 결과를 한 단계 펼친다 |
| reduce | 전체 원소를 하나의 결과로 합친다 |
| reduceByKey | 같은 키의 값들을 결합한다 |
| groupByKey | 같은 키의 값들을 모은다 |
| mapValues | 키를 유지하고 값만 변환한다 |
| count() | 원소 수를 반환한다 |
| collect() | 전체 원소를 Python 리스트로 가져온다 |
| take(2) | 최대 2개 원소를 Python 리스트로 가져온다 |

```python
# 모든 측정소의 측정시각 토큰 빈도
times = rdd.map(lambda x: x["dataTime"])
tokens = times.flatMap(lambda x: x.split())
pairs = tokens.map(lambda x: (x, 1))
freq = pairs.reduceByKey(lambda a, b: a + b)

print("전체 토큰 수:", tokens.count())
print("토큰별 빈도:", freq.collect())
print("서로 다른 토큰 수:", freq.count())
```

- pairs.reduce(lambda a,b:a+b)는 튜플 전체를 이어 붙인다. 키별 빈도에는 reduceByKey를 사용한다.
- flatMap의 M, reduceByKey의 B와 K 등 대소문자를 지킨다.
- filter/map 같은 변환의 오류가 count/collect 같은 액션 실행 시 드러날 수 있다.

## 6. groupByKey와 mapValues: 리스트 자체와 내부 숫자 구분

```python
import builtins

# 서울 측정소 딕셔너리 RDD를 입력으로 사용
pairs_rdd = rdd.map(lambda x: (x["SAREA_NM"], x["FPM"]))
groups = pairs_rdd.groupByKey().mapValues(list)
avg = groups.mapValues(lambda x: builtins.sum(x) / len(x))
```

```text
("도심권", 14.0), ("도심권", 9.0), ("도심권", 18.0)
    ↓ groupByKey().mapValues(list)
("도심권", [14.0, 9.0, 18.0])
    ↓ mapValues로 평균 계산
("도심권", 13.666666666666666)
```

- 마지막 mapValues에서 x는 [14.0, 9.0, 18.0]이라는 list다.
- x[0]은 14.0이라는 float다.
- map은 원소 전체를 받고, mapValues는 값 부분만 받는다.
- groupByKey 뒤에 reduceByKey를 다시 적용해도 리스트 내부의 평균을 계산해주지 않는다.

## 7. 정렬과 상위 2개

```python
# (권역명, 평균)을 권역명 오름차순으로 정렬
print(avg.sortByKey().collect())

# 평균 기준 내림차순: 평균을 키 위치로 이동
changed = avg.map(lambda x: (x[1], x[0]))
print(changed.sortByKey(False).take(2))
```

- sortByKey() / sortByKey(True): 키 오름차순.
- sortByKey(False): 키 내림차순.
- 숫자 문자열을 정렬하기 전에 int/float로 변환한다.
- 이 map에서 x는 (권역명, 평균) 튜플 전체다.

## 8. 파일 읽기와 저장

```python
r.text             # 응답 문자열: r.txt 아님
r.json()           # JSON 응답을 Python 자료로 변환
json.loads(text)   # JSON 문자열을 Python 자료로 변환

# 에어코리아 정상 응답만 저장
if "response" in data and data["response"]["header"]["resultCode"] == "00":
    with open(filePath, "w", encoding="utf-8") as f:
        f.write(r.text)
```

- 텍스트 파일의 write에는 문자열을 전달한다. 딕셔너리 data를 그대로 전달하지 않는다.
- and는 왼쪽이 거짓이면 오른쪽을 평가하지 않는다.
- spark.sparkContext.textFile(filePath)는 파일을 줄 단위로 읽는다.
- spark.sparkContext.parallelize(rows)는 이미 있는 자료를 RDD로 만든다.
- parallelize(filePath)는 파일을 읽지 않고 경로 문자열의 문자들을 원소로 만든다.
- 문자열 RDD의 first()는 문자열 하나, take(1)은 원소 하나를 담은 리스트다.

## 9. pandas 대괄호

```python
city = df[df["SAREA_NM"] == "도심권"]
print(city[["MSRSTN_NM", "PM", "FPM"]])
print(df.groupby("SAREA_NM")[["PM", "FPM"]].mean())
```

- df["SAREA_NM" == "도심권"]은 문자열끼리 비교해 df[False]가 되므로 잘못이다.
- 여러 열은 열 이름 리스트를 넣어 선택한다: df[["PM", "FPM"]].
- 전체 권역 통계라면 필터한 city가 아닌 전체 df를 그룹화한다.

## 10. MongoDB 문서별 합계 초기화

```python
results = collection.find({"SBWY_ROUT_LN_NM": "1호선"})
total_sum = 0
count = 0

for doc in results:
    total = 0  # 문서마다 초기화
    for hour in [7, 8]:
        field_name = f"HR_{hour}_GET_ON_NOPE"
        total += int(doc[field_name])
    print(doc["STTN"], total)
    total_sum += total
    count += 1

print(count, total_sum)
```

문서별 total은 문서 반복문 안에서, 전체 total_sum은 반복문 밖에서 초기화한다.

## 11. 오류가 나면 먼저 볼 것

- KeyError: 실제 키 이름과 응답 구조를 확인한다. 서울 PM과 에어코리아 pm10Value를 혼동하지 않는다.
- list indices must be integers: 리스트를 딕셔너리처럼 접근했는지 확인한다.
- str object has no attribute findtext: XML 요소 대신 문자열을 꺼냈는지 확인한다.
- float object is not callable: sum 같은 함수 이름에 숫자를 저장했는지 확인한다.
- RDD object is not iterable: 일반 for 대신 RDD 연산을 사용한다.

```python
print(type(sum))  # float라면 내장 함수 이름을 가린 상태

import builtins
print(builtins.sum([10, 20]))
```

sum/str/list를 변수명으로 쓰지 않는다. 합계는 total 또는 pm_total로 명명한다.
RDD 메서드는 r_rdd.count()처럼 호출한다. r.rdd_count()가 아니다.
오류 로그가 길면 Python의 KeyError/TypeError 메시지와 자신의 코드 줄부터 찾는다.

## 12. 오픈북 검색 위치

| 자료 | 절 또는 검색어 |
|---|---|
| [study/ds1_intro_python_numpy.ipynb](study/ds1_intro_python_numpy.ipynb) | 함수, 리스트, 딕셔너리, 함수 인자로 받은 리스트 |
| [study/ds2_open_data.ipynb](study/ds2_open_data.ipynb) | findall, small_data, groupby, MongoClient, 파일 저장 성공과 API 정상 응답 |
| [study/ds3_spark_rdd.ipynb](study/ds3_spark_rdd.ipynb) | S.6 RDD API, 단순통계기능, flatMap, reduceByKey, 묶어서, sortByKey(False) |
| [study/ds3_spark_rdd.ipynb](study/ds3_spark_rdd.ipynb) | 그룹화한 리스트의 평균과 mapValues, sum/len 이름 덮어쓰기 |

수업 원문은 study/ 노트북에서 확인한다. 이 문서의 코드는 복습용으로 축약·변형한 예시이며, 각 예시의 입력 데이터와 선행 변수를 확인해서 사용한다.

**막히면 먼저 두 가지: 현재 원소의 자료형은 무엇인가? 필요한 필드 이름은 무엇인가?**
