# تقرير نتائج Hybrid Big Data ELT Pipeline

## 1. ملخص التنفيذ

تم تنفيذ خط معالجة بيانات هجين يعتمد على:

- Python Batch للملفات الصغيرة.
- PySpark للملفات الكبيرة.
- MongoDB لتخزين البيانات الخام والمنظفة والمعزولة.

يتم إدخال جميع البيانات أولًا إلى `orders_raw` قبل تطبيق أي عملية Cleaning أو Validation.

---

## 2. نتيجة الملف الكبير

اسم الملف:

`orders_huge_mixed_quality.csv`

حجم الملف:

`12650.32 MB`

عدد السجلات:

`30,000,000`

المحرك المستخدم:

`PySpark`

عدد Partitions:

`99`

---

## 3. Raw Load Metrics

| المقياس | النتيجة |
|---|---:|
| Rows Read | 30,000,000 |
| Raw Loaded | 30,000,000 |
| Load Time | 535.4824 ثانية |
| MongoDB Write Time | 488.4852 ثانية |
| Throughput | 56,024.25 سجل/ثانية |

تم تحميل جميع السجلات إلى:

`orders_raw`

قبل بدء التنظيف والتصنيف.

---

## 4. Cleaning & Validation Results

| النتيجة | العدد |
|---|---:|
| Valid | 0 |
| Corrected | 27,271,346 |
| Quarantine | 2,728,654 |
| Total Processed | 30,000,000 |

### Consistency Check

```text
27,271,346 + 2,728,654 = 30,000,000
النتيجة:

PASS

وهذا يعني أن جميع السجلات الموجودة في Raw حصلت على Processing Outcome ولم يتم فقد أي سجل.

5. Validated Business Records

عدد سجلات الأعمال الفريدة داخل:

orders_validated

هو:

27,079,084

بينما عدد السجلات التي انتهت بنتيجة Validated/Corrected هو:

27,271,346

الفرق سببه وجود قيم order_id مكررة في المصدر.

تم استخدام:

Stable Business Key = order_id
Unique Index
Upsert

لمنع إنشاء Business Records مكررة.

6. Upsert Results

في التشغيل الأساسي:

العملية    العدد
Inserted    27,079,084
Updated    192,262
Unchanged    0

وعند إعادة المعالجة لم يتم إنشاء Business Records جديدة، مما يثبت عمل Idempotency.

7. Error Case Counts
Error Code    العدد
BAD_JSON    419,906
MISSING_CUSTOMER_ID    419,474
INVALID_EMAIL    418,709
INVALID_ORDER_DATE    210,524
INVALID_STATUS    210,194
INVALID_CURRENCY    210,190
INVALID_PHONE    210,042
INVALID_QTY    210,021
INVALID_ITEM_TOTAL    210,018
EMPTY_ITEMS    209,934
INVALID_MONEY    209,432
MISSING_ORDER_ID    209,392
NEGATIVE_PAYMENT_AMOUNT    209,114

ملاحظة:

قد يحتوي سجل Quarantine الواحد على أكثر من Error Code، لذلك مجموع Error Counts قد يكون أكبر من عدد سجلات Quarantine.

8. مقارنة Python Batch وPySpark

تم تنفيذ المقارنة على نفس العينة:

100,000 rows

Python Batch
المقياس    النتيجة
Rows    100,000
Batch Size    5,000
Time    3.5297 ثانية
Throughput    28,330.67 سجل/ثانية
PySpark
المقياس    النتيجة
Rows    100,000
Partitions    8
Time    6.2685 ثانية
Throughput    15,952.69 سجل/ثانية
النتيجة

Python Batch كان أسرع على العينة الصغيرة بحوالي:

1.78x

وذلك بسبب Startup وExecution Overhead الخاص بـSpark.

أما PySpark فهو مناسب للملفات الكبيرة بسبب:

Parallel Processing
Partition-based Execution
DataFrame API
قابلية أفضل للتوسع
9. اختبارات المشروع

تم تشغيل الاختبارات باستخدام:

python -m pytest tests -v

النتيجة النهائية:

12 passed
0 failed
10. MongoDB Collections

تم استخدام:

orders_raw
orders_validated
orders_quarantine

كما تم تطبيق:

Unique Index على order_id
Schema Validation على orders_validated
11. Spark UI Evidence

تم توثيق:

Jobs
Stages
Tasks
Input Partitions
عدد السجلات المعالجة

وفي اختبار Spark UI:

INPUT_PARTITIONS = 8
ROWS = 600000
12. ملفات النتائج

النتائج التفصيلية محفوظة في:

reports/results.json

ومقارنة الأداء محفوظة في:

reports/performance_comparison.json

والصور والأدلة محفوظة في:

reports/screenshots/

