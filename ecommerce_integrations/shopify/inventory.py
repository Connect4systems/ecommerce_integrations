# -*- coding: utf-8 -*-
from collections import Counter

import frappe
from frappe.utils import cint, create_batch, now
from pyactiveresource.connection import ResourceNotFound
from shopify.resources import InventoryLevel, Variant

from ecommerce_integrations.controllers.inventory import (
    get_inventory_levels,
    update_inventory_sync_status,
)
from ecommerce_integrations.controllers.scheduling import need_to_run
from ecommerce_integrations.shopify.connection import temp_shopify_session
from ecommerce_integrations.shopify.constants import MODULE_NAME, SETTING_DOCTYPE
from ecommerce_integrations.shopify.utils import create_shopify_log


def _log(status: str, message: str, data=None, method: str = "update_inventory_on_shopify"):
    create_shopify_log(status=status, message=message, request_data=data or {}, method=method)


def _filter_to_flagged_items(inventory_levels):
    """Keep rows for Items with custom_sync_to_shopify = 1 and not disabled."""
    if not inventory_levels:
        return []

    ecom_names = [d.ecom_item for d in inventory_levels if getattr(d, "ecom_item", None)]
    if not ecom_names:
        return []

    ecom_rows = frappe.get_all(
        "Ecommerce Item",
        filters={"name": ["in", ecom_names], "integration": MODULE_NAME},
        fields=["name", "erpnext_item_code"],
    )
    ecom_to_item = {r["name"]: r["erpnext_item_code"] for r in ecom_rows if r.get("erpnext_item_code")}

    allowed_items = set(
        frappe.get_all(
            "Item",
            filters={"name": ["in", list(ecom_to_item.values())], "custom_sync_to_shopify": 1, "disabled": 0},
            pluck="name",
        )
    )

    filtered = []
    for d in inventory_levels:
        erp_item = ecom_to_item.get(getattr(d, "ecom_item", ""))
        if erp_item in allowed_items:
            filtered.append(d)

    return filtered


def _commit_row_and_continue(d, synced_on):
    try:
        if getattr(d, "ecom_item", None):
            update_inventory_sync_status(getattr(d, "ecom_item"), time=synced_on)
    except Exception:
        pass
    frappe.db.commit()


def _log_batch_status(inventory_levels) -> None:
    """Per-row status summary in Shopify Log."""
    log_message = "variant_id,location_id,status,failure_reason\n"
    log_message += "\n".join(
        f"{getattr(d,'variant_id','')},{getattr(d,'shopify_location_id','')},"
        f"{getattr(d,'status','')},{getattr(d,'failure_reason','') or ''}"
        for d in inventory_levels
    )

    statuses = [getattr(d, "status", "Failed") for d in inventory_levels]
    stats = Counter(statuses)
    total = max(len(inventory_levels), 1)
    pct = stats.get("Success", 0) / total

    status = "Success" if pct == 1 else ("Partial Success" if pct > 0 else "Failed")
    _log(status, f"Updated {pct * 100}% items\n\n{log_message}")


@temp_shopify_session
def upload_inventory_data_to_shopify(inventory_levels, warehouse_map) -> None:
    """Push inventory to Shopify for each row (Default Warehouse only)."""
    synced_on = now()

    for batch in create_batch(inventory_levels, 50):
        for d in batch:
            # force single mapped location for Default Warehouse
            try:
                d.shopify_location_id = int(warehouse_map[d.warehouse])
            except Exception:
                d.status = "Failed"
                d.failure_reason = f"No numeric Shopify Location for ERP Warehouse: {d.warehouse}"
                _commit_row_and_continue(d, synced_on)
                continue

            try:
                if not getattr(d, "variant_id", None):
                    d.status = "Failed"
                    d.failure_reason = "Missing variant_id in Ecommerce Item mapping."
                    _commit_row_and_continue(d, synced_on)
                    continue

                variant = Variant.find(d.variant_id)
                inventory_item_id = getattr(variant, "inventory_item_id", None)
                if not inventory_item_id:
                    d.status = "Failed"
                    d.failure_reason = f"Shopify variant {d.variant_id} has no inventory_item_id."
                    _commit_row_and_continue(d, synced_on)
                    continue

                available = cint(d.actual_qty) - cint(d.reserved_qty)  # Shopify wants integers
                InventoryLevel.set(
                    location_id=d.shopify_location_id,
                    inventory_item_id=inventory_item_id,
                    available=int(available),
                )
                update_inventory_sync_status(d.ecom_item, time=synced_on)
                d.status = "Success"

            except ResourceNotFound:
                update_inventory_sync_status(d.ecom_item, time=synced_on)
                d.status = "Not Found"
                d.failure_reason = (
                    f"Variant or Location not found. variant_id={getattr(d,'variant_id',None)} "
                    f"loc={getattr(d,'shopify_location_id',None)}"
                )
            except Exception as e:
                d.status = "Failed"
                d.failure_reason = str(e)

            frappe.db.commit()

        _log_batch_status(batch)


def _get_numeric_location_id(setting, default_wh: str) -> int:
    erp_to_shop = setting.get_erpnext_to_integration_wh_mapping() or {}
    if not default_wh or default_wh not in erp_to_shop:
        raise Exception(f"No Shopify Location mapping for Default Warehouse: {default_wh}")
    try:
        return int(erp_to_shop[default_wh])
    except Exception:
        raise Exception(
            f"Shopify Location ID must be numeric. Got '{erp_to_shop[default_wh]}' for '{default_wh}'."
        )


def _run_push_for_default_warehouse(setting) -> None:
    """Core one-way push using ONLY the Default Warehouse on Shopify Setting."""
    if not setting.is_enabled() or not setting.update_erpnext_stock_levels_to_shopify:
        return

    default_wh = getattr(setting, "warehouse", None)
    if not default_wh:
        _log("Error", "Default Warehouse is not set on Shopify Setting (field: warehouse).")
        return

    location_id = None
    try:
        location_id = _get_numeric_location_id(setting, default_wh)
    except Exception as e:
        _log("Error", str(e))
        return

    # inventory rows for only the Default Warehouse
    inventory_levels = get_inventory_levels((default_wh,), MODULE_NAME)
    if not inventory_levels:
        _log("Success", f"No inventory rows found for Default Warehouse: {default_wh}")
        return

    # keep only flagged items
    inventory_levels = _filter_to_flagged_items(inventory_levels)
    if not inventory_levels:
        _log("Success", "No flagged items to sync (custom_sync_to_shopify=1 & not disabled).")
        return

    # push
    upload_inventory_data_to_shopify(inventory_levels, {default_wh: location_id})


def update_inventory_on_shopify() -> None:
    """Scheduled job entrypoint – one-way push from ERPNext to Shopify."""
    setting = frappe.get_doc(SETTING_DOCTYPE)
    if not need_to_run(SETTING_DOCTYPE, "inventory_sync_frequency", "last_inventory_sync"):
        return
    _run_push_for_default_warehouse(setting)


def update_inventory_on_shopify_now() -> None:
    """Immediate manual push (can be called from console or other code)."""
    setting = frappe.get_doc(SETTING_DOCTYPE)
    _run_push_for_default_warehouse(setting)
