# Execution Environments dataset for big_test1
# Format: {'managed': 't' or 'f'}
# - 't' = True = default execution environment
# - 'f' = False = custom execution environment

execution_environments = [
    {'managed': 't'},  # Default EE 1
    {'managed': 'f'},  # Custom EE 1
    {'managed': 't'},  # Default EE 2
    {'managed': 'f'},  # Custom EE 2
    {'managed': 'f'},  # Custom EE 3
    {'managed': 't'},  # Default EE 3
    {'managed': 'f'},  # Custom EE 4
    {'managed': 't'},  # Default EE 4
]

# Expected totals:
# - execution_environments_total: 8
# - execution_environments_default_total: 4 (managed='t')
# - execution_environments_custom_total: 4 (managed='f')
