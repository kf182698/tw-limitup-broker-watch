from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def _normalize(text: Optional[Any]) -> str:
    """
    將輸入轉成統一格式的字串，用來做比對：
    - None / NaN -> ""（空字串）
    - 其餘一律轉成 str，去除空白並轉小寫
    """
    if text is None:
        return ""
    s = str(text)
    return s.lower().replace(" ", "").strip()


def match_target_broker(
    broker_name: Optional[Any] = None,
    broker_code: Optional[Any] = None,
    target_brokers: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    判斷某一筆分點資料的券商是否在「目標券商清單」內。

    Parameters
    ----------
    broker_name : 任意型別
        來源頁面的券商名稱（可能是 None / NaN / 字串）
    broker_code : 任意型別
        來源頁面的券商代號（可能是 None / NaN / 字串）
    target_brokers : list[dict]
        目標券商清單，例如每個 dict 內有:
        {
            "name": "凱基台北",
            "code": "9200"  # 若有
            ...
        }

    Returns
    -------
    (matched, meta) : (bool, Optional[dict])
        matched 為 True 表示有命中，meta 為命中的那一個目標券商資料。
    """
    if not target_brokers:
        return False, None

    norm_name = _normalize(broker_name)
    norm_code = _normalize(broker_code)

    # 若名稱與代號都空，直接判定不可能命中
    if not norm_name and not norm_code:
        return False, None

    for item in target_brokers:
        # 允許 item 是 dict，也預留未來直接用字串的可能性
        if isinstance(item, dict):
            t_name = _normalize(item.get("name"))
            t_code = _normalize(item.get("code"))
        else:
            # 如果 brokers.yaml 未來改成只給名稱字串
            t_name = _normalize(item)
            t_code = ""

        # 先比對名稱
        if norm_name and t_name and norm_name == t_name:
            return True, item

        # 再比對代號
        if norm_code and t_code and norm_code == t_code:
            return True, item

    return False, None