using System;

namespace DepFac.MySqlToPostgreSql
{
	public class ColumnDefinition
	{
		public string Name { get; set; }

		public string DataType { get; set; }

		public bool IsNullable { get; set; }

		public double? Precision { get; set; }

		public int Position { get; set; }

		public Type Type { get; set; }
	}
}
