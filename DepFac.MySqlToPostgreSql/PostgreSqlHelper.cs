using System;
using Npgsql;

namespace DepFac.MySqlToPostgreSql
{
	public class PostgreSqlHelper
	{
		private const string TableNamesQuery = @"SELECT table_name
				                                 FROM information_schema.tables
				                                 WHERE table_schema = 'public'
				                                    AND table_type = 'BASE TABLE'";

		private const string ColumnNamesQuery = @"SELECT tables.table_name,
				                                         columns.column_name,
				                                         columns.data_type,
				                                         columns.is_nullable,
				                                         COALESCE(columns.character_maximum_length, columns.numeric_precision, columns.datetime_precision),
				                                         columns.ordinal_position
				                                  FROM information_schema.columns
				                                  INNER JOIN information_schema.tables ON tables.table_name = columns.table_name
				                                  WHERE tables.table_schema = 'public'
				                                     AND tables.table_type = 'BASE TABLE'
				                                     AND tables.table_schema = columns.table_schema";

		public const string RemoveConstraintQuery = @"SELECT 'ALTER TABLE ""'||relname||'"" DROP CONSTRAINT ""'||conname||'"";'
				                                      FROM pg_constraint
				                                      INNER JOIN pg_class ON conrelid = pg_class.oid
				                                      INNER JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
				                                      WHERE contype = 'f'
				                                      ORDER BY nspname,
				                                               relname,
				                                               conname";

		public const string AddContraintQuery = @"SELECT 'ALTER TABLE ""'||relname||'"" ADD CONSTRAINT ""'||conname||'"" '|| pg_get_constraintdef(pg_constraint.oid)||';'
				                                  FROM pg_constraint
				                                  INNER JOIN pg_class ON conrelid = pg_class.oid
				                                  INNER JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
				                                  WHERE contype = 'f'
				                                  ORDER BY nspname DESC,
				                                           relname DESC,
				                                           conname DESC";

		public static string GetTableNamesQuery()
		{
			return string.Format(TableNamesQuery);
		}

		public static string GetColumnNamesQuery()
		{
			return string.Format(ColumnNamesQuery);
		}

		public static Type DataTypeToType(string dataType)
		{
			switch (dataType)
			{
				case "timestamp without time zone":
				case "timestamp with time zone":
					return typeof(DateTime);
				case "bigint":
					return typeof(long);
				case "bytea":
					return typeof(byte[]);
				case "text":
				case "character varying":
				case "character":
					return typeof(string);
				case "boolean":
					return typeof(bool);
				case "uuid":
					return typeof(Guid);
				case "integer":
					return typeof(int);
				case "numeric":
					return typeof(decimal);
				default:
					throw new ArgumentOutOfRangeException(nameof(dataType), $"Unrecognized data type : '{dataType}'");
			}
		}

		public static void AddToSerializer(NpgsqlCopySerializer serializer, ColumnDefinition columnDefinition, object value)
		{
			if (value == null)
			{
				serializer.AddNull();
			}
			else if (columnDefinition.Type == typeof (DateTime))
			{
				serializer.AddDateTime(((DateTime?) value).Value);
			}
			else if (columnDefinition.Type == typeof (long))
			{
				serializer.AddInt64(((long?) value).Value);
			}
			else if (columnDefinition.Type == typeof (byte[]))
			{
				serializer.AddNull();
			}
			else if (columnDefinition.Type == typeof (string))
			{
				serializer.AddString((string) value);
			}
			else if (columnDefinition.Type == typeof (bool))
			{
				serializer.AddBool(((bool?) value).Value);
			}
			else if (columnDefinition.Type == typeof (int))
			{
				serializer.AddInt32(((int?) value).Value);
			}
			else if (columnDefinition.Type == typeof (decimal))
			{
				serializer.AddNumber((double) ((decimal?) value).Value);
			}
			else if (columnDefinition.Type == typeof (Guid))
			{
				serializer.AddString(((Guid?) value).Value.ToString());
			}
			else
			{
				throw new ArgumentOutOfRangeException(nameof(columnDefinition),
					$"Not supported type in PostgreSQL : '{columnDefinition.Type}'");
			}
		}
	}
}
