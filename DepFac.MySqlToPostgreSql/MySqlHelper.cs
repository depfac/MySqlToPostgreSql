using System;
using System.Data;

namespace DepFac.MySqlToPostgreSql
{
	public class MySqlHelper
	{
		private const string TableNamesQuery = @"SELECT TABLE_NAME
				                                 FROM information_schema.TABLES 
				                                 WHERE TABLE_SCHEMA = '{0}'
				                                    AND TABLE_TYPE = 'BASE TABLE'";

		private const string ColumnNamesQuery = @"SELECT TABLES.TABLE_NAME,
				                                         COLUMNS.COLUMN_NAME,
				                                         COLUMNS.DATA_TYPE,
				                                         COLUMNS.IS_NULLABLE,
				                                         COALESCE(COLUMNS.CHARACTER_MAXIMUM_LENGTH, COLUMNS.NUMERIC_PRECISION),
				                                         COLUMNS.ORDINAL_POSITION
				                                  FROM information_schema.COLUMNS
				                                  INNER JOIN information_schema.TABLES ON TABLES.TABLE_NAME = COLUMNS.TABLE_NAME
				                                  WHERE TABLES.TABLE_SCHEMA = '{0}'
				                                     AND TABLES.TABLE_TYPE = 'BASE TABLE'
				                                     AND TABLES.TABLE_SCHEMA = COLUMNS.TABLE_SCHEMA";

		public static string GetTableNamesQuery(string schema)
		{
			return string.Format(TableNamesQuery, schema);
		}

		public static string GetColumnNamesQuery(string schema)
		{
			return string.Format(ColumnNamesQuery, schema);
		}

		public static Type DataTypeToType(string dataType)
		{
			switch (dataType)
			{
				case "datetime":
					return typeof (DateTime);
				case "bigint":
					return typeof (long);
				case "longblob":
				case "mediumblob":
					return typeof (byte[]);
				case "text":
				case "varchar":
				case "char":
					return typeof (string);
				case "boolean":
					return typeof (bool);
				case "int":
				case "tinyint":
					return typeof (int);
				case "decimal":
					return typeof (decimal);
				default:
					throw new ArgumentOutOfRangeException(nameof(dataType), $"Unrecognized data type : '{dataType}'");
			}
		}

		public static object GetValueFromDataReader(IDataReader reader, ColumnDefinition columnDefinition)
		{
			int position = columnDefinition.Position - 1;

			object o;
			if (columnDefinition.Type == typeof(DateTime))
			{
				o = reader.IsDBNull(position) ? (DateTime?)null : reader.GetDateTime(position);
			}
			else if (columnDefinition.Type == typeof(long))
			{
				o = reader.IsDBNull(position) ? (long?)null : Convert.ToInt64(reader.GetValue(position));
			}
			else if (columnDefinition.Type == typeof(byte[]))
			{
				o = reader.IsDBNull(position) ? new byte[0] : (byte[]) reader.GetValue(position);
			}
			else if (columnDefinition.Type == typeof(string))
			{
				o = reader.IsDBNull(position) ? null : reader.GetString(position);
			}
			else if (columnDefinition.Type == typeof(bool))
			{
				o = reader.IsDBNull(position) ? (bool?)null : reader.GetBoolean(position);
			}
			else if (columnDefinition.Type == typeof(int))
			{
				o = reader.IsDBNull(position) ? (int?)null : reader.GetInt32(position);
			}
			else if (columnDefinition.Type == typeof(decimal))
			{
				o = reader.IsDBNull(position) ? (decimal?)null : reader.GetDecimal(position);
			}
			else
			{
				throw new ArgumentOutOfRangeException(nameof(columnDefinition), $"Not supported type in MySQL : '{columnDefinition.Type}'");
			}
			return o;
		}
	}
}
