# مشروع Hybrid Big Data ELT Pipeline

مشروع لمعالجة بيانات الطلبات غير النظيفة باستخدام بنية ELT هجينة تعتمد على:

- Python Batch Processing
- PySpark
- MongoDB
- MongoDB Spark Connector

---

## 1. هدف المشروع

يهدف المشروع إلى بناء خط معالجة بيانات قادر على التعامل مع ملفات CSV صغيرة وكبيرة تحتوي على بيانات غير نظيفة، مع الحفاظ على البيانات الأصلية قبل أي عملية تنظيف.

يقوم النظام بالمهام التالية:

1. تحديد حجم ملف الإدخال تلقائيًا.
2. اختيار محرك المعالجة المناسب حسب حجم الملف.
3. إدخال جميع البيانات أولًا إلى `orders_raw`.
4. تطبيق قواعد التنظيف والتحقق بعد مرحلة Raw.
5. حفظ البيانات السليمة أو المصححة في `orders_validated`.
6. عزل البيانات غير القابلة للتصحيح بأمان في `orders_quarantine`.
7. منع تكرار الطلبات باستخدام `order_id`.
8. استخدام Upsert وUnique Index لتحقيق Idempotency.
9. تسجيل Metrics وError Counts لكل عملية تشغيل.
10. توفير اختبارات وأدلة تنفيذ للمشروع.

---

## 2. معمارية النظام

```text
Dirty CSV
    |
    v
File Router
    |
    +-----------------------------+
    |                             |
<= 200 MB                     > 200 MB
Python Batch                    PySpark
    |                             |
    +-------------+---------------+
                  |
                  v
             orders_raw
                  |
                  v
       Cleaning + Validation
             /        \
            /          \
           v            v
orders_validated   orders_quarantine
        |
        v
 Idempotent Upsert

Metrics
   |
   v
reports/results.json
3. File Router

يقوم File Router بفحص حجم الملف تلقائيًا.

الحد المستخدم:

200 MB

منطق الاختيار:

حجم الملف <= 200 MB  → Python Batch
حجم الملف > 200 MB   → PySpark

ويقوم الـRouter بطباعة:

اسم الملف.
حجم الملف.
المحرك المختار.
سبب اختيار المحرك.
4. Python Batch Loader

يستخدم لمعالجة الملفات الصغيرة.

المميزات
قراءة CSV بأسلوب Streaming.
عدم تحميل الملف كاملًا في الذاكرة.
تقسيم البيانات إلى دفعات.
استخدام insert_many() للكتابة إلى MongoDB.
إظهار تقدم كل Batch.
حساب زمن التنفيذ.
حساب Throughput.

حجم الدفعة المستخدم:

batch_size = 5000
5. PySpark Loader

يستخدم لمعالجة الملفات الكبيرة.

المميزات
استخدام Spark DataFrame API.
استخدام Fixed Schema.
قراءة الحقول الخام كـString.
عدم استخدام Pandas لمعالجة الملف الكبير.
عدم استخدام inferSchema للملف الكبير.
استخدام MongoDB Spark Connector.
تنفيذ متوازٍ باستخدام Partitions.
تسجيل عدد الـPartitions.
حساب زمن التنفيذ وThroughput.
التعامل الصحيح مع items_json الموجود داخل CSV.
6. Collections في MongoDB

يستخدم المشروع ثلاث Collections أساسية.

orders_raw

تحتوي على البيانات الأصلية قبل التنظيف.

من أهم الحقول:

id_run
file_source
at_ingested
engine_used
record_raw
processing_outcome

يتم الاحتفاظ بـrecord_raw كما وصل من المصدر قبل تطبيق قواعد التنظيف.

orders_validated

تحتوي على البيانات السليمة أو التي تم تصحيحها بأمان.

من أهم الحقول:

order_id
record_clean
quality_status
corrections
last_id_run
last_raw_id
file_source

يوجد Unique Index على:

order_id

وذلك لمنع وجود Business Records مكررة.

orders_quarantine

تحتوي على السجلات التي لا يمكن تصحيحها بأمان.

من أهم الحقول:

السجل الخام.
error_codes
error_details
corrections_attempted
id_run
engine_used

لا يتم حذف السجلات غير الصالحة، بل يتم عزلها مع توضيح سبب الخطأ.

7. قواعد التنظيف والتحقق

يطبق المشروع مجموعة من القواعد الثابتة والمحددة، منها:

إزالة المسافات الزائدة.
التحقق من وجود order_id.
التحقق من وجود customer_id.
توحيد صيغ التاريخ المعروفة.
توحيد الأرقام العربية والقيم الرقمية.
إزالة فواصل الآلاف عند الحاجة.
توحيد العملات المعروفة.
اكتشاف القيم المالية السالبة غير الآمنة.
توحيد أرقام الهاتف اليمنية.
تصحيح البريد الإلكتروني عندما يكون الخطأ واضحًا وآمنًا.
توحيد القيم التصنيفية المعروفة.
التحقق من صحة items_json.
توحيد الكميات الرقمية النصية.
رفض الكميات السالبة.
التحقق من إجماليات العناصر.
إعادة حساب إجمالي الطلب عندما تكون المكونات صالحة.

لا يتم تخمين أي قيمة غير واضحة.

8. Audit Trail

عند إجراء أي تصحيح يتم تسجيل تفاصيل التعديل.

الحقول المستخدمة:

field
original_value
corrected_value
rule_code

مثال:

{
  "field": "customer_email",
  "original_value": "user@@mail..com",
  "corrected_value": "user@mail.com",
  "rule_code": "email_double_at"
}

وبذلك يمكن معرفة القيمة الأصلية والقيمة الجديدة وقاعدة التصحيح المستخدمة.

9. Upsert وIdempotency

المفتاح التجاري الثابت المستخدم هو:

order_id

ويتم تطبيق Unique Index على order_id.

سلوك Upsert:

order_id جديد        → Insert
order_id موجود       → Update
نفس النتيجة النهائية → Unchanged

إعادة معالجة نفس البيانات لا تؤدي إلى إنشاء Business Records مكررة.

10. قاعدة الاتساق

يجب أن ينتهي كل سجل موجود في Raw إلى نتيجة معالجة واحدة فقط.

قاعدة الاتساق:

run_raw_count
=
run_valid_count
+
run_corrected_count
+
run_quarantine_count

ويتم تسجيل نتيجة الفحص على شكل:

PASS / FAIL
11. نتيجة معالجة الملف الكبير

اسم الملف:

orders_huge_mixed_quality.csv

حجم الملف:

12650.32 MB

عدد السجلات:

30,000,000

المحرك المستخدم:

PySpark

عدد الـPartitions:

99
نتيجة تحميل Raw
Rows Read       = 30,000,000
Raw Loaded      = 30,000,000
Load Time       = 535.4824 seconds
Write Time      = 488.4852 seconds
Throughput      = 56,024.25 rows/second
نتيجة Cleaning وValidation
Valid       = 0
Corrected   = 27,271,346
Quarantine  = 2,728,654
فحص الاتساق
27,271,346 + 2,728,654 = 30,000,000


PASS

عدد Business Records الفريدة داخل orders_validated:

27,079,084

الفرق بين عدد النتائج المصححة وعدد Business Records سببه وجود order_id مكرر داخل المصدر.

يتم استخدام Upsert لتحديث السجل الموجود بدل إنشاء Duplicate جديد.

12. Error Case Counts

> الأرقام والأكواد في هذا القسم هي أدلة تشغيل تاريخية من Phase 1 قبل مواءمة الأكواد مع Reference Practice Dataset.

من أهم أنواع الأخطاء التي تم اكتشافها:

BAD_JSON                     = 419906
MISSING_CUSTOMER_ID          = 419474
INVALID_EMAIL                = 418709
INVALID_ORDER_DATE           = 210524
INVALID_STATUS               = 210194
INVALID_CURRENCY             = 210190
INVALID_PHONE                = 210042
INVALID_QTY                  = 210021
INVALID_ITEM_TOTAL           = 210018
EMPTY_ITEMS                  = 209934
INVALID_MONEY                = 209432
MISSING_ORDER_ID             = 209392
NEGATIVE_PAYMENT_AMOUNT      = 209114

يمكن أن يحتوي السجل الواحد داخل Quarantine على أكثر من Error Code.

13. مقارنة Python Batch وPySpark

تم تنفيذ مقارنة على نفس عينة البيانات المكونة من:

100,000 rows
Python Batch
Rows        = 100,000
Batch Size  = 5,000
Time        = 3.5297 seconds
Throughput  = 28,330.67 rows/second
PySpark
Rows        = 100,000
Partitions  = 8
Time        = 6.2685 seconds
Throughput  = 15,952.69 rows/second
النتيجة
Python Batch أسرع على العينة الصغيرة بحوالي 1.78x

ويرجع ذلك إلى وجود Startup وExecution Overhead في Spark.

أما عند التعامل مع البيانات الكبيرة فإن PySpark يوفر:

Parallel Processing
Partition-based Processing
DataFrame API
قابلية أفضل للتوسع

تفاصيل المقارنة محفوظة في:

reports/performance_comparison.json
14. Metrics

يتم حفظ نتائج التشغيل في:

reports/results.json

وتشمل المقاييس:

run_id
file_name
file_size_mb
engine_used
rows_read
raw_loaded
valid_count
corrected_count
quarantine_count
elapsed_seconds
throughput
batch_size
partitions
error_case_counts
inserted_count
updated_count
unchanged_count
15. الاختبارات

لتشغيل الاختبارات:

python -m pytest tests -v

نتيجة الاختبار الأخيرة:

12 passed
0 failed

وتغطي الاختبارات:

Cleaning Rules
Classification
Bad JSON
Missing Business Key
Negative Quantity
Arabic Money
Date Validation
Consistency
Deterministic Processing
Stable Business Key
16. التشغيل الرئيسي

نقطة التشغيل الرئيسية للمشروع:

python -m src.main --input "path/to/orders.csv"

بعد ذلك يقوم File Router تلقائيًا باختيار:

python_batch

أو:

pyspark

حسب حجم ملف الإدخال.

17. هيكل المشروع
project/
|
|-- config/
|   `-- settings.py
|
|-- src/
|   |-- main.py
|   |-- file_router.py
|   |-- create_small_sample.py
|   |-- batch_loader.py
|   |-- spark_loader.py
|   |-- quality_rules.py
|   |-- elt_pipeline.py
|   |-- mongo_setup.py
|   `-- metrics.py
|
|-- tests/
|   |-- test_cleaning_rules.py
|   |-- test_classification.py
|   |-- test_consistency.py
|   `-- test_idempotency.py
|
|-- reports/
|   |-- results.json
|   |-- results.md
|   |-- performance_comparison.json
|   `-- screenshots/
|
|-- docs/
|   `-- architecture.md
|
|-- requirements.txt
|-- .gitignore
`-- README.md
18. أدلة التنفيذ

يتم حفظ صور وأدلة التنفيذ داخل:

reports/screenshots/

وتشمل الأدلة:

MongoDB Collections Overview
Raw Layer
Validated Records
Quarantine Records
Unique Index
Schema Validation
Spark Jobs
Spark Stages
Spark Tasks
Spark Partitions
19. التقنيات المستخدمة
Python 3.12
PySpark 4.2
MongoDB
MongoDB Compass
MongoDB Spark Connector
Java 17
PyTest
Git
GitHub



---

---

# Phase 2 - Final Project

تمت إضافة متطلبات **Phase 2** إلى **نفس مشروع النصفي ونفس GitHub Repository**. لم يتم إنشاء مشروع منفصل.

## 20. Practical Queries

الملف:

```text
src/phase2_queries.py
```

يوفر المشروع خمسة Practical Queries:

```text
orders_by_city
orders_by_status
customer_orders_by_date
orders_by_payment_method
high_value_orders
```

عرض أسماء الاستعلامات:

```powershell
python -c "from src.phase2_queries import list_queries; print(list_queries())"
```

كل Query قابل للاستدعاء مباشرة من Python وكذلك من FastAPI.

---

## 21. Indexes

الملف:

```text
src/phase2_indexes.py
```

فهارس Phase 2:

```text
ix_phase2_city
ix_phase2_status
ix_phase2_customer_date
```

### `ix_phase2_city`

الحقل:

```text
record_clean.city
```

السبب: دعم Equality Filter في `orders_by_city`.

### `ix_phase2_status`

الحقل:

```text
record_clean.status
```

السبب: دعم Equality Filter في `orders_by_status`.

### `ix_phase2_customer_date`

Compound Index على:

```text
record_clean.customer_id
record_clean.order_date
```

السبب: دعم تصفية طلبات العميل حسب `customer_id` مع الفلترة/الترتيب حسب `order_date`.

إنشاء الفهارس:

```powershell
python -m src.phase2_indexes
```

---

## 22. Explain Before / After Indexes

الملف:

```text
src/phase2_explain.py
```

يستخدم:

```text
explain(..., verbosity="executionStats")
```

على ثلاثة Queries:

```text
orders_by_city
orders_by_status
customer_orders_by_date
```

التشغيل:

```powershell
python -m src.phase2_explain
```

السكربت:

1. يحذف **فهارس Phase 2 فقط**.
2. يشغل Explain قبل الفهارس.
3. يعيد إنشاء الفهارس.
4. يشغل Explain بعد الفهارس.
5. يضمن بقاء فهارس Phase 2 موجودة في النهاية.

### نتيجة فعلية: `orders_by_city`

قبل:

```text
n_returned: 2705130
docs_examined: 27079084
keys_examined: 0
execution_time_ms: 141991
uses_ixscan: False
```

بعد:

```text
n_returned: 2705130
docs_examined: 2705130
keys_examined: 2705130
execution_time_ms: 123104
uses_ixscan: True
```

### نتيجة فعلية: `orders_by_status`

قبل:

```text
n_returned: 4513704
docs_examined: 27079084
keys_examined: 0
execution_time_ms: 136381
uses_ixscan: False
```

بعد:

```text
n_returned: 4513704
docs_examined: 4513704
keys_examined: 4513704
execution_time_ms: 134591
uses_ixscan: True
```

### نتيجة فعلية: `customer_orders_by_date`

قبل:

```text
n_returned: 1
docs_examined: 27079084
keys_examined: 0
execution_time_ms: 145380
uses_ixscan: False
```

بعد:

```text
n_returned: 1
docs_examined: 1
keys_examined: 1
execution_time_ms: 4
uses_ixscan: True
```

الخلاصة: قبل الفهارس كانت الاستعلامات تعتمد على Collection Scan، وبعدها استخدمت MongoDB `IXSCAN` وانخفض عدد المستندات التي يجب فحصها.

> على قاعدة البيانات الكبيرة قد يستغرق Explain قبل الفهارس عدة دقائق بسبب فحص ملايين المستندات.

---

## 23. Aggregation Reports

الملف:

```text
src/phase2_aggregations.py
```

التقارير الخمسة:

```text
sales_by_city
orders_by_status_summary
payment_methods_summary
delivery_types_summary
sales_by_month
```

عرض الأسماء:

```powershell
python -c "from src.phase2_aggregations import list_aggregations; print(list_aggregations())"
```

تشغيل تقرير محدد:

```powershell
python -c "from src.phase2_aggregations import run_aggregation; print(run_aggregation('sales_by_city'))"
```

تم اختبار التقارير عمليًا على قاعدة التدريب:

```text
sales_by_city             -> 7 groups
orders_by_status_summary  -> 6 groups
payment_methods_summary   -> 3 groups
delivery_types_summary    -> 2 groups
sales_by_month            -> 6 groups
```

---

## 24. Materialized Views

الملف:

```text
src/phase2_materialized_views.py
```

يوجد اثنان Materialized Views:

```text
mv_daily_sales_summary
mv_city_sales_summary
```

### `mv_daily_sales_summary`

يلخص:

```text
order_count
total_sales
```

حسب اليوم.

### `mv_city_sales_summary`

يلخص:

```text
order_count
total_sales
```

حسب المدينة.

### Full Refresh

أول تشغيل يبني الـMaterialized Views من المصدر كاملًا.

### Incremental Refresh

بعد البناء الأول يستخدم المشروع Watermark:

```text
last_source_updated_at
```

ويعالج فقط السجلات التي تغيرت بعد آخر Refresh.

تشغيل Refresh:

```powershell
python -c "from src.phase2_materialized_views import refresh_materialized_views; print(refresh_materialized_views())"
```

اختبار فعلي على قاعدة التدريب:

```text
First Refresh:
mode = full
changed_orders = 17000

Second Refresh without changes:
mode = incremental
changed_orders = 0
```

Collections المساعدة:

```text
mv_order_contributions
mv_refresh_state
```

---

## 25. Scheduled Jobs

الملف:

```text
src/phase2_jobs.py
```

يوجد Jobان فعليان:

### `refresh_materialized_views`

الجدولة:

```text
every 30 minutes
```

الوظيفة: تشغيل Refresh للـMaterialized Views.

### `aggregation_snapshot`

الجدولة:

```text
daily at 23:00
```

الوظيفة: تشغيل التقارير الخمسة وحفظ Snapshot.

### Manual Run

```powershell
python -c "from src.phase2_jobs import run_job; print(run_job('refresh_materialized_views'))"
```

أو:

```powershell
python -c "from src.phase2_jobs import run_job; print(run_job('aggregation_snapshot'))"
```

### Job Logs

يتم تسجيل:

```text
started
success
failure
started_at
ended_at
details
```

داخل:

```text
phase2_job_logs
```

تم اختبار Manual Run وLogging فعليًا بنجاح.

---

## 26. FastAPI

الملف:

```text
src/api.py
```

تشغيل الخادم:

```powershell
python -m uvicorn src.api:app --host 127.0.0.1 --port 8000
```

Swagger:

```text
http://127.0.0.1:8000/docs
```

OpenAPI:

```text
http://127.0.0.1:8000/openapi.json
```

### Endpoints المطلوبة

```text
GET  /health
POST /ingest
POST /indexes
GET  /queries
GET  /queries/{name}
GET  /aggregations
GET  /aggregations/{name}
POST /refresh-mv
GET  /jobs
POST /jobs/{name}/run
```

### `GET /health`

يفحص API واتصال MongoDB.

### `POST /ingest`

يستخدم **نفس Pipeline الخاص بـPhase 1**.

Body:

```json
{
  "input_path": "path/to/orders.csv"
}
```

المسار:

```text
File Router
-> Python Batch / PySpark
-> orders_raw
-> ELT
-> orders_validated / orders_quarantine
-> Metrics
```

### `POST /indexes`

ينشئ فهارس Phase 2.

### `GET /queries`

يعرض أسماء Practical Queries.

### `GET /queries/{name}`

أمثلة:

```text
/queries/orders_by_city?city=Sanaa&limit=10
/queries/orders_by_status?status=Delivered&limit=10
/queries/customer_orders_by_date?customer_id=C-100&limit=10
/queries/orders_by_payment_method?payment_method=cash&limit=10
/queries/high_value_orders?min_total=10000&limit=10
```

### `GET /aggregations`

يعرض أسماء التقارير.

### `GET /aggregations/{name}`

مثال:

```text
/aggregations/sales_by_city
```

### `POST /refresh-mv`

يشغل Refresh للـMaterialized Views.

### `GET /jobs`

يعرض الـJobs والجداول الزمنية.

### `POST /jobs/{name}/run`

Manual Run للـJob.

مثال:

```text
POST /jobs/refresh_materialized_views/run
```

---

## 27. API Verification

تم اختبار الـAPI فعليًا:

```text
/health                         -> 200
/indexes                        -> 200
/queries/...                    -> 200
/aggregations/...               -> 200
/refresh-mv                     -> 200
/jobs                           -> 200
/jobs/{name}/run                -> 200
/openapi.json                   -> 200
/ingest                         -> 200
```

كما تم اختبار `/ingest` على Reference Practice Dataset وحقق:

```text
Valid       = 12000
Corrected   = 5000
Quarantine  = 3000
```

---

## 28. Environment Variables

الملف:

```text
.env.example
```

المحتوى:

```text
MONGO_URI=mongodb://127.0.0.1:27017
MONGO_DATABASE=midterm_data_pipeline
```

لا يحتوي Secrets.

يقرأ `config/settings.py` القيم من Environment Variables، وإذا لم يتم تعيينها يستخدم القيم الافتراضية.

مثال PowerShell:

```powershell
$env:MONGO_DATABASE="my_test_database"
python -m pytest -q
Remove-Item Env:MONGO_DATABASE
```

---

## 29. Installation

تثبيت المتطلبات:

```powershell
python -m pip install -r requirements.txt
```

الحزم الرئيسية:

```text
pymongo==4.17.0
pyspark==4.2.0
pytest==9.1.1
fastapi==0.127.0
uvicorn==0.40.0
APScheduler==3.11.3
```

MongoDB يجب أن تكون متاحة على القيمة الموجودة في `MONGO_URI`.

---

## 30. Quick Start للمقيّم

### 1. تثبيت المتطلبات

```powershell
python -m pip install -r requirements.txt
```

### 2. تشغيل الاختبارات

```powershell
python -m pytest -q
```

النتيجة الحالية:

```text
12 passed
```

### 3. إنشاء Phase 2 Indexes

```powershell
python -m src.phase2_indexes
```

### 4. تشغيل Explain

```powershell
python -m src.phase2_explain
```

### 5. تشغيل FastAPI

```powershell
python -m uvicorn src.api:app --host 127.0.0.1 --port 8000
```

ثم:

```text
http://127.0.0.1:8000/docs
```

### 6. تشغيل Ingest مباشرة بدون API

```powershell
python -m src.main --input "path/to/orders.csv"
```

---

## 31. Reference Practice Dataset Verification

تم اختبار القواعد الحالية على Reference Practice Dataset الذي وفره الدكتور:

```text
Input / Raw       = 20,000
Valid             = 12,000
Corrected         = 5,000
Quarantine        = 3,000
orders_validated  = 17,000
Consistency       = True
```

توزيع Quarantine:

```text
12 error types x 250 = 3,000
```

الأكواد:

```text
corrupted_items_json
email_missing_domain
empty_items
invalid_date_impossible
invalid_phone_too_short
missing_customer_id
missing_item_sku
missing_order_id
multiple_conflicting_errors
negative_quantity
unknown_currency
unknown_order_status
```

توزيع Corrections:

```text
10 correction types x 500 = 5,000
```

الأكواد:

```text
arabic_digits_delivery_cost
arabic_digits_payment_amount
currency_arabic_name
date_dd_mm_yyyy
email_double_at
phone_with_country_code
price_with_thousands_commas
qty_as_string_in_items
status_extra_spaces
total_amount_mismatch_recomputable
```

> هذه الأرقام أدلة اختبار فقط وليست Hardcoded داخل الكود.

---

## 32. هيكل المشروع النهائي

```text
midterm-data-pipeline/
|
|-- config/
|   `-- settings.py
|
|-- src/
|   |-- main.py
|   |-- file_router.py
|   |-- create_small_sample.py
|   |-- batch_loader.py
|   |-- spark_loader.py
|   |-- quality_rules.py
|   |-- elt_pipeline.py
|   |-- mongo_setup.py
|   |-- metrics.py
|   |
|   |-- phase2_queries.py
|   |-- phase2_indexes.py
|   |-- phase2_explain.py
|   |-- phase2_aggregations.py
|   |-- phase2_materialized_views.py
|   |-- phase2_jobs.py
|   `-- api.py
|
|-- tests/
|   |-- test_cleaning_rules.py
|   |-- test_classification.py
|   |-- test_consistency.py
|   `-- test_idempotency.py
|
|-- reports/
|   |-- results.json
|   |-- results.md
|   |-- performance_comparison.json
|   `-- screenshots/
|
|-- docs/
|   `-- architecture.md
|
|-- .env.example
|-- .gitignore
|-- requirements.txt
`-- README.md
```

---

## 33. Phase 2 Requirement Checklist

| المتطلب | الحالة |
|---|---|
| 5 Practical Queries | ✅ |
| 3 Indexes على الأقل | ✅ |
| Compound Index | ✅ |
| Explain على 3 Queries قبل/بعد | ✅ |
| Index Rationale + Effect | ✅ |
| 5 Aggregation Reports | ✅ |
| 2 Materialized Views | ✅ |
| Incremental Refresh | ✅ |
| 2 Scheduled Jobs | ✅ |
| Job Schedules | ✅ |
| Manual Job Run | ✅ |
| Job Logging | ✅ |
| FastAPI | ✅ |
| Swagger `/docs` | ✅ |
| `GET /health` | ✅ |
| `POST /ingest` | ✅ |
| `POST /indexes` | ✅ |
| `GET /queries` | ✅ |
| `GET /queries/{name}` | ✅ |
| `GET /aggregations` | ✅ |
| `GET /aggregations/{name}` | ✅ |
| `POST /refresh-mv` | ✅ |
| `GET /jobs` | ✅ |
| `POST /jobs/{name}/run` | ✅ |
| Updated `README.md` | ✅ |
| Updated `requirements.txt` | ✅ |
| `.env.example` without secrets | ✅ |
| Same GitHub Repository | ✅ |

---

## 34. التقنيات المستخدمة

```text
Python 3.12
PyMongo
PySpark 4.2
MongoDB
MongoDB Compass
MongoDB Spark Connector
FastAPI
Uvicorn
APScheduler
PyTest
Java 17
Git
GitHub
```

---

## 35. الخلاصة

المشروع يحتوي على مرحلتين داخل نفس Repository:

```text
Phase 1:
Hybrid Big Data ELT Pipeline
+ Data Quality
+ Python Batch / PySpark
+ MongoDB
+ Metrics
+ Idempotent Business Records

Phase 2:
Practical Queries
+ Indexes
+ Explain executionStats
+ Aggregations
+ Materialized Views
+ Incremental Refresh
+ Scheduled Jobs
+ FastAPI
+ Swagger
```

يمكن للمقيّم تثبيت المتطلبات، تشغيل MongoDB، تشغيل الاختبارات، ثم تشغيل أي مكون من Phase 2 أو استخدام Swagger وفق الأوامر الموضحة أعلاه.
