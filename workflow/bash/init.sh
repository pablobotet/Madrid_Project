# Install additional Python libraries
/databricks/python/bin/pip install pandas numpy boto3 geopy openpyxl

# Add logging for troubleshooting
{
  echo "Starting init script..."
  # Your commands here
  echo "Init script completed successfully."
} >> /databricks/driver/logs/init-script.log 2>&1