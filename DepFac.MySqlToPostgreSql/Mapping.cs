using System;
using System.Collections.Generic;

namespace DepFac.MySqlToPostgreSql
{
	public class Mapping
	{
		public string MySqlTableName { get; set; }

		public string PostgreSqlTableName { get; set; }

		public IDictionary<ColumnDefinition, ColumnDefinition> MappingColumns { get; set; }

		public static readonly IDictionary<Type, IDictionary<Type, Func<object, object>>> ConversionTable;

		static Mapping()
		{
			ConversionTable = new Dictionary<Type, IDictionary<Type, Func<object, object>>>
			                  {
				                  {
					                  typeof (string), new Dictionary<Type, Func<object, object>>()
				                  },
				                  {
					                  typeof (int), new Dictionary<Type, Func<object, object>>()
				                  }
			                  };
			ConversionTable[typeof(string)].Add(typeof(Guid), ConvertStringToGuid);
			ConversionTable[typeof(int)].Add(typeof(bool), ConvertIntegerToBoolean);
		}

		private static object ConvertStringToGuid(object value)
		{
			if (value != null)
			{
				Guid id;
				if (Guid.TryParse((string) value, out id))
				{
					return id;
				}
			}
			return null;
		}

		private static object ConvertIntegerToBoolean(object value)
		{
			if (value != null)
			{
				int i;
				if (int.TryParse(value.ToString(), out i))
				{
					return i == 1;
				}
			}
			return null;
		}
	}
}
