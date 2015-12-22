using System;

namespace DepFac.MySqlToPostgreSql
{
	public static class UtilExtensions
	{
		public static bool EqualsInvariantIgnoreCase(this string left, string right)
		{
			return string.Equals(left, right, StringComparison.OrdinalIgnoreCase);
		}
    }
}
