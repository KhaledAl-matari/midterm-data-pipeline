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
  "rule_code": "EMAIL_REPEATED_SYMBOLS"
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

11 passed
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

python src/main.py --input "path/to/orders.csv"

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
