<div dir="rtl" markdown="1">

# Aggify به فارسی

**Aggify** یک ODM مدرن و async-first برای MongoDB است که API آن شبیه Django ORM است، ولی به Django وابسته **نیست**؛
همان مدل‌ها در FastAPI، gRPC (`grpc.aio`)، Django (viewهای sync و async)، اسکریپت‌ها و workerها کار می‌کنند.

## شروع سریع

```bash
pip install aggify                 # هسته (فقط pymongo)
pip install "aggify[fastapi]"      # Pydantic و FastAPI
pip install "aggify[django]"
pip install "aggify[grpc]"
```

```python
import aggify
from aggify import Document, StringField, IntField, Q, F, Count, Sum

aggify.connect("mongodb://localhost:27017/shop")      # فقط تنظیمات را ثبت می‌کند؛ اتصال lazy است

class Book(Document):
    title = StringField(required=True)
    year = IntField(min_value=0)

    class Meta:
        collection = "books"
        ordering = ["year"]

# sync
Book.objects.create(title="Dune", year=1965)
Book.objects.filter(Q(year__lt=1900) | Q(title__icontains="du")).order_by("-year")[:10]

# async (هر متد I/O یک نسخه‌ی a-دار دارد)
book = await Book.objects.aget(title="Dune")
book.year = 1966
await book.asave()        # فقط {"$set": {"year": 1966}} فرستاده می‌شود
```

## چرا کوئری‌ها کوتاه می‌شوند؟

کوئری‌های پیچیده‌ی Mongo (join، group، facet، window) به‌جای نوشتن pipeline دستی، با همان سبک Django ساخته می‌شوند:

```python
(Order.objects.filter(status="paid", customer__country="DE")      # $lookup + $unwind + $match خودکار
      .values("customer__name")
      .annotate(revenue=Sum("total"), orders=Count())              # $group
      .filter(orders__gte=3)                                       # HAVING
      .order_by("-revenue")[:10])
```

هر زمان `qs.query` را بزنید، دقیقاً همان چیزی که به MongoDB فرستاده می‌شود را می‌بینید. برای مرحله‌هایی که queryset ندارد
(`$facet`, `$bucket`, `$setWindowFields`, `$graphLookup` ...) از `Aggify(Model)` استفاده کنید.
نمونه‌های واقعی و اجراشده در [Cookbook](cookbook.md) هستند.

## استفاده در پروژه‌های دیگر

* **FastAPI**: `aggify.contrib.fastapi` (`lifespan`, `get_or_404`, `Pagination`, مپ خطاها به HTTP) و
  `pydantic_model(Model)` برای ساخت خودکار schema ورودی/خروجی.
* **gRPC**: `@grpc_errors` خطاهای Aggify را به status code درست gRPC تبدیل می‌کند؛ `fill_message` / `message_to_kwargs`
  بین protobuf و مدل تبدیل می‌کنند.
* **Django**: `INSTALLED_APPS += ["aggify.contrib.django"]` و تنظیم `AGGIFY = {...}`؛ `get_object_or_404`، Paginator، فرم،
  میدلور تراکنش، و `ObjectIdField` برای نگه‌داشتن id مونگو داخل مدل‌های SQL جنگو.

## امنیت

* **NoSQL injection**: مقدارها قبل از رسیدن به کوئری توسط نوع فیلد بررسی می‌شوند؛ `filter(username={"$ne": ""})` خطا می‌دهد.
* **Regex**: `contains` و بقیه ورودی را escape می‌کنند؛ فقط `regex` صریح الگو می‌پذیرد (با سقف طول).
* **JavaScript سمت سرور** (`$where`, `$function`, `$accumulator`) به‌صورت پیش‌فرض مسدود است.
* نام فیلدها اعتبارسنجی می‌شوند، Mass-assignment با `TypeError` رد می‌شود، رمز URI در لاگ ماسک می‌شود.
* سقف‌های قابل تنظیم: `max_limit`، `default_max_time_ms`، `max_in_list`.

جزئیات کامل: [Security](security.md).

## تست

`assert_num_queries(n)` و `capture_commands()` برای شمارش کوئری‌ها (مثل `assertNumQueries` جنگو)،
`qs.query` / `pipeline.to_list()` برای تست بدون دیتابیس، و تست‌های differential با Hypothesis که ۲۵۰ کوئری تصادفی را
با MongoDB واقعی و یک مرجع پایتونی مقایسه می‌کنند. جزئیات: [Testing](testing.md).

## مهاجرت

* از MongoEngine: [راهنمای مهاجرت](migration/mongoengine.md) — داده‌ها سازگارند و می‌توان مدل‌به‌مدل مهاجرت کرد.
* از Aggify نسخه‌ی 0.x: [تغییرات](migration/aggify-0.md).

</div>
