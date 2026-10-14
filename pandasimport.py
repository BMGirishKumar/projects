import pandas as pd

# Constructing a realistic raw operational data payload with built-in anomalies
mock_wms_data = {
    'snapshot_date': ['2026-10-14', '2026-10-14', '2026-10-14', '2026-10-14', '2026-10-14'],
    'sku': [' TS-01 ', 'EL-05', 'AP-02', 'TS-01', 'BR-99 '],               # Trailing spaces & duplicate 'TS-01'
    'product_name': ['Tech Shirt', 'Wireless Mouse', None, 'Tech Shirt', 'Broken Item'], # Contains a NULL name
    'category': ['apparel', 'electronics', 'Apparel', 'apparel', 'Storage'], # Inconsistent casing
    'quantity_on_hand': [150, 45, 12, 100, -5],                             # Contains an illegal negative value
    'unit_cost_usd': [25.00, 19.99, 45.50, 25.00, 10.00]
}

# Output directly to your workspace
df_mock = pd.DataFrame(mock_wms_data)
df_mock.to_csv("wms_inventory_dump.csv", index=False)
print("📌 File 'wms_inventory_dump.csv' has been generated in your active directory!")
