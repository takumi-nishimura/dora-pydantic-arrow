# dora-pydantic-arrow 仕様書

## 概要
`dora-pydantic-arrow` は、Pydantic モデルおよびそれを含む Python オブジェクト（`list`、`dict`、`tuple` など）を Apache Arrow 形式へ変換し、`dora-rs` によるデータ送受信に対応させるためのライブラリです。型安全性・性能・相互運用性を重視し、Arrow から Python/Pydantic への逆変換もサポートします。

本ライブラリは以下を目的とします：
- **送信**：Python/Pydantic → Arrow Array / RecordBatch / Table
- **受信**：Arrow Array / RecordBatch / Table → Python/Pydantic
- **dora-rs** の `send_output` / `dora_event["value"]` との直接互換

---

## 対応環境
| 項目 | 内容 |
|------|------|
| Python | 3.7 以上（推奨 3.11、`dora-rs-cli` に準拠） |
| Pydantic | v2 系必須 |
| 依存ライブラリ | `pyarrow`, `pydantic`, `uuid-utils`, （任意：`numpy`） |

---

## 基本 API
```python
# 送信（Python/Pydantic → Arrow）
to_arrow(
    obj,
    *,
    schema: pa.Schema | None = None,
    config: DAConfig | None = None
) -> pa.Array | pa.RecordBatch | pa.Table

# 受信（Arrow → Python/Pydantic）
from_arrow(
    data: pa.Array | pa.RecordBatch | pa.Table,
    *,
    type_hint: type | None = None,
    validate: bool = True,
    config: DAConfig | None = None
)

# Pydantic モデルから Arrow スキーマ生成
schema_from_model(
    model_type: type[BaseModel],
    *,
    config: DAConfig | None = None
) -> pa.Schema

# 大規模データ対応：逐次追加で RecordBatch を構築
class BatchBuilder:
    def __init__(self, *, schema: pa.Schema | type[BaseModel] | None = None, batch_size: int = 65536, config: DAConfig | None = None): ...
    def append(self, obj) -> None: ...
    def to_record_batch(self) -> pa.RecordBatch: ...
    def clear(self) -> None: ...
```

---

## コンフィグ（DAConfig）
```python
from dataclasses import dataclass
from typing import Literal

@dataclass
class DAConfig:
    datetime_policy: Literal["normalize_utc", "preserve_tz", "error_on_naive"] = "normalize_utc"
    enum_encoding: Literal["auto"] = "auto"  # Enum はユーザー定義に従う
    dict_key_policy: Literal["string_only"] = "string_only"
    union_encoding: Literal["tagged_struct", "arrow_dense_union"] = "tagged_struct"
    decimal_precision: int = 38
    decimal_scale: int = 9
    ndarray_encoding: Literal["nested_list", "fixed_size_list_if_static"] = "nested_list"
    fast_path_skip_validation: bool = False
```

---

## 型マッピング規則
| Python/Pydantic 型 | Arrow 型 | 備考 |
|--------------------|-----------|------|
| `BaseModel` | `Struct` | フィールド名＝モデル名 |
| `list[T]` | `List<T>` | 再帰的に変換 |
| `dict[str, V]` | `Map<string, V>` | キーは string 固定、値はユーザー定義 |
| `tuple[T1, T2,...]` | `Struct(f0,f1,...)` | 固定長ベクトルはオプションで FixedSizeList |
| `Optional[T]` | `nullable` | null 許可 |
| `Enum(IntEnum 等)` | 整数型（int32 など） | 値に基づき型決定 |
| `Enum(StrEnum 等)` | `string` | |
| `Union[A,B,...]` | tagged struct（既定） | `__type__` + `__value__` |
| `datetime` | `timestamp[us, tz=UTC]` | `normalize_utc` 既定 |
| `date` / `time` | `date32` / `time64[us]` | |
| `Decimal` | `decimal128(p,s)` | precision/scale 可変 |
| `bytes` | `binary` | ゼロコピー |
| `numpy.ndarray` | `List<T>` | ネスト構造 |
| `Any` | 非推奨 | json 化オプトイン |

---

## UUID（UUIDv7 / uuid-utils）対応
- **ユーザーの型定義に従う**（例：`UUID7` from `pydantic` / `uuid-utils`）。
- `fixed_size_binary[16]` を既定（高性能・時間順序維持）。
- Arrow スキーマメタに以下を保存：
  - `uuid.version=7`
  - `uuid.encoding=binary16`
  - `uuid_utils=true`

### サンプル
```python
from enum import Enum
from pydantic import BaseModel, UUID7, Field
import uuid, uuid_utils

enum = Enum('Kind', {'DEFAULT': 1, 'ERROR': 2})

class MyModel(BaseModel):
    id: UUID7 = Field(default_factory=lambda: uuid.UUID(bytes=uuid_utils.uuid7().bytes))
    kind: enum

rows = [MyModel(kind=enum.DEFAULT), MyModel(kind=enum.ERROR)]
rb = to_arrow(rows)  # Arrow RecordBatch
back = from_arrow(rb, type_hint=list[MyModel])
```

---

## スキーマメタデータ
| メタキー | 内容 |
|----------|------|
| `pydantic_model_fqn` | フル修飾モデル名 |
| `pydantic_version` | 使用バージョン |
| `model_schema_hash` | スキーマハッシュ |
| `datetime_policy` | 日時ポリシー |
| `uuid.version` / `encoding` | UUID 情報 |

---

## スキーマ進化とバリデーション
- フィールド追加：nullable で許容
- 型変更：互換変換 or `SchemaMismatchError`
- 受信時：`validate=True` で Pydantic 検証

---

## エラーモデル
| 例外名 | 意味 |
|--------|------|
| `SchemaMismatchError` | Arrow ↔ Pydantic 不一致 |
| `UnsupportedTypeError` | 未対応型 |
| `ValidationError` | Pydantic 検証失敗 |

---

## パフォーマンス設計
- ゼロコピーを優先（Arrow / numpy バッファ共有）
- 大規模対応：`BatchBuilder` による逐次構築

---

## dora-rs 連携
- `send_output` には `to_arrow(...)` の結果を直接渡す想定
- `dora_event["value"]` から Arrow Array を受け、`from_arrow(...)` で復元

---

## 今後の拡張候補
- Arrow Flight / gRPC 対応
- Rust 側実装とのメタ仕様共有
- Pydantic v1 互換レイヤ追加
