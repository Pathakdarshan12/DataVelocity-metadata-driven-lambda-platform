import pandas as pd

# Read input CSV
df = pd.read_csv("D:\Programming\Projects\DataVelocity\data\order_item\order_item_04-01-2025.csv")
df.columns = df.columns.str.strip()
# Columns you want to remove
columns_to_remove = [
    "OrderItemID",
    "CreatedDate",
    "ModifiedDate"
]

# Drop specified columns
df = df.drop(columns=columns_to_remove, errors="raise")

# Write updated CSV
df.to_csv("order_item_04_01_2025.csv", index=False)

print("Updated CSV saved as output.csv")
