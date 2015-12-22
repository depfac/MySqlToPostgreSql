using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using log4net;
using log4net.Config;
using MySql.Data.MySqlClient;
using Npgsql;
using NpgsqlTypes;

namespace DepFac.MySqlToPostgreSql
{
	public class Program
	{
		private static readonly ILog Logger = LogManager.GetLogger(typeof (Program));

		public static void Main(string[] args)
		{
			XmlConfigurator.Configure();

			string mySqlConnectionString = ConfigurationManager.AppSettings["mySql:connectionString"];
			string postgreSqlConnectionString = ConfigurationManager.AppSettings["postgreSql:connectionString"];

			using (var mySqlConnection = new MySqlConnection(mySqlConnectionString))
			using (var postgreSqlConnection = new NpgsqlConnection(postgreSqlConnectionString))
			{
				mySqlConnection.Open();
				postgreSqlConnection.Open();

				try
				{
					#region Mappging MySql - Postgres & Create missing in Postgres

					IList<string> mySqlTableNames = GetTableNames(mySqlConnection, MySqlHelper.GetTableNamesQuery(mySqlConnection.Database));
					IDictionary<string, IList<ColumnDefinition>> mySqlColumnDefinitions =
						GetColumnDefinitionPerTables(mySqlConnection, mySqlTableNames, MySqlHelper.GetColumnNamesQuery(mySqlConnection.Database),
							MySqlHelper.DataTypeToType);
					Logger.Info($"Definition of tables and columns of database '{mySqlConnection.Database}' acquired (MySQL)");

					IList<string> postgreSqlTableNames = GetTableNames(postgreSqlConnection, PostgreSqlHelper.GetTableNamesQuery());
					IDictionary<string, IList<ColumnDefinition>> postgreSqlColumnDefinitions =
						GetColumnDefinitionPerTables(postgreSqlConnection, postgreSqlTableNames, PostgreSqlHelper.GetColumnNamesQuery(),
							PostgreSqlHelper.DataTypeToType);
					Logger.Info($"Definition of tables and columns of database '{postgreSqlConnection.Database}' acquired (PostgreSQL)");

					IList<Mapping> mappings = new List<Mapping>();
					foreach (var mySqlTableName in mySqlTableNames)
					{
						string postgreSqlTableName =
							postgreSqlTableNames.SingleOrDefault(
								x => x.Replace("_", string.Empty).EqualsInvariantIgnoreCase(mySqlTableName.Replace("_", string.Empty)));
						if (postgreSqlTableName == null)
						{
							// TODO: create table + add in column definition
							Logger.Info($"Table '{mySqlTableName}' doesn't exists in PostgreSQL, let's create it (doesn't work for now)");
						}

						IList<ColumnDefinition> mySqlTableColumns = mySqlColumnDefinitions[mySqlTableName];
						IList<ColumnDefinition> postgreSqlTableColumns = postgreSqlColumnDefinitions[postgreSqlTableName];
						IDictionary<ColumnDefinition, ColumnDefinition> mappingColumns =
							new Dictionary<ColumnDefinition, ColumnDefinition>();
						foreach (var mySqlColumn in mySqlTableColumns.OrderBy(x => x.Position))
						{
							ColumnDefinition postgreSqlColumn = FindEquivalentColumn(mySqlColumn, postgreSqlTableColumns);
							if (postgreSqlColumn == null)
							{
								// TODO: alter table + add in column definition
								Logger.Info($"Column '{mySqlColumn}' of table '{postgreSqlTableName}' doesn't exists in PostgreSQL, let's create it (doesn't work for now)");
							}
							// TODO: check nullable, precision, foreign key, index, primary key???
							mappingColumns.Add(mySqlColumn, postgreSqlColumn);
						}
						if (mySqlTableColumns.Count != postgreSqlTableColumns.Count)
						{
							throw new Exception(
								$"MySQL table '{mySqlTableName}' doest not have the same definition as PostgreSQL table '{postgreSqlTableName}'");
						}

						mappings.Add(new Mapping
						         {
							         MySqlTableName = mySqlTableName,
							         PostgreSqlTableName = postgreSqlTableName,
							         MappingColumns = mappingColumns
						         });
					}

					#endregion

					#region Postgres constraints

					IList<string> removeConstraintQueries = new List<string>();
					using (IDbCommand removeConstraintCommand = postgreSqlConnection.CreateCommand())
					{
						removeConstraintCommand.CommandText = PostgreSqlHelper.RemoveConstraintQuery;
						removeConstraintCommand.CommandTimeout = 30;
						using (IDataReader reader = removeConstraintCommand.ExecuteReader())
						{
							while (reader.Read())
							{
								removeConstraintQueries.Add(reader.GetString(0));
							}
						}
					}

					IList<string> addConstraintQueries = new List<string>();
					using (IDbCommand addConstraintCommand = postgreSqlConnection.CreateCommand())
					{
						addConstraintCommand.CommandText = PostgreSqlHelper.AddContraintQuery;
						addConstraintCommand.CommandTimeout = 30;
						using (IDataReader reader = addConstraintCommand.ExecuteReader())
						{
							while (reader.Read())
							{
								addConstraintQueries.Add(reader.GetString(0));
							}
						}
					}

					#endregion

					#region Remove Postgres constraints

					Logger.Info("Remove foreign key constraints in PostgreSQL");
					foreach (var query in removeConstraintQueries)
					{
						using (IDbCommand removeConstraintCommand = postgreSqlConnection.CreateCommand())
						{
							Logger.Debug(query);
							removeConstraintCommand.CommandText = query;
							removeConstraintCommand.CommandTimeout = 60;
							removeConstraintCommand.ExecuteReader().Dispose();
						}
					}

					#endregion

					#region Migrate data

					foreach (var mapping in mappings)
					{
						Logger.Info($"Migrate data from '{mapping.MySqlTableName}' to '{mapping.PostgreSqlTableName}'");
						using (IDbCommand deleteCommand = postgreSqlConnection.CreateCommand())
						{
							deleteCommand.CommandText = $@"DELETE FROM {mapping.PostgreSqlTableName}";
							deleteCommand.CommandTimeout = 2000;
							deleteCommand.ExecuteReader().Dispose();
						}
						Logger.Info($"Records of table '{mapping.PostgreSqlTableName}' deleted");

						var serializer = new NpgsqlCopySerializer(postgreSqlConnection);
						using (var copyCommand = postgreSqlConnection.CreateCommand())
						{
							copyCommand.CommandText = $"COPY {mapping.PostgreSqlTableName} FROM stdin";
							copyCommand.CommandTimeout = 2000;

							var copy = new NpgsqlCopyIn(copyCommand, postgreSqlConnection, serializer.ToStream);
							try
							{
								copy.Start();
								using (IDbCommand selectCommand = mySqlConnection.CreateCommand())
								{
									selectCommand.CommandText = $@"SELECT * FROM {mapping.MySqlTableName}";
									selectCommand.CommandTimeout = 2000;
									using (IDataReader reader = selectCommand.ExecuteReader())
									{
										int count = 0;
										while (reader.Read())
										{
											Serialize(serializer, reader, mapping.MappingColumns);
											if (++count%1000 == 0)
											{
												Logger.Info($"{count} records inserted in table '{mapping.PostgreSqlTableName}'");
											}
										}
										Logger.Info($"{count} records inserted in table '{mapping.PostgreSqlTableName}'");
									}
								}
							}
							catch (Exception e)
							{
								Logger.Error(e);
								copy.Cancel("Undo copy");
								throw;
							}
							finally
							{
								try
								{
									copy.CopyStream?.Close();
									copy.End();
								}
								catch (Exception e)
								{
									Logger.Error(e);
								}
							}

							#region Workaround for blobs

							var blobColumns = mapping.MappingColumns.Where(x => x.Value.Type == typeof (byte[])).ToList();
							if (blobColumns.Any())
							{
								// TODO: not always the first column = primary key => improve column definition???
								ColumnDefinition postgresIdColumn = mapping.MappingColumns.Values.Single(x => x.Position == 1);
								ColumnDefinition mysqlIdColumn = mapping.MappingColumns.Keys.Single(x => x.Position == 1);

								string updateQuery = $"UPDATE {mapping.PostgreSqlTableName} SET";
								string selectQuery = $"SELECT {mysqlIdColumn.Name},";
								int cpt = 0;
								foreach (var blobColumn in blobColumns)
								{
									if (cpt > 0)
									{
										updateQuery += ",";
										selectQuery += ",";
									}
									updateQuery += $" {blobColumn.Value.Name} = :bytesData{++cpt}";
									selectQuery += $" {blobColumn.Key.Name}";
								}
								updateQuery += $" WHERE {postgresIdColumn.Name} = :idData";

								using (IDbCommand selectCommand = mySqlConnection.CreateCommand())
								{
									selectCommand.CommandText = selectQuery + $" FROM {mapping.MySqlTableName}";
									selectCommand.CommandTimeout = 2000;

									using (IDataReader reader = selectCommand.ExecuteReader())
									{
										int count = 0;
										while (reader.Read())
										{
											using (var updateCommand = postgreSqlConnection.CreateCommand())
											{
												updateCommand.CommandText = updateQuery;
												updateCommand.CommandTimeout = 2000;

												// TODO: not always uuid
												NpgsqlParameter idParam = new NpgsqlParameter(":idData", NpgsqlDbType.Uuid) {Value = reader.GetValue(0)};
												updateCommand.Parameters.Add(idParam);

												for (int pos = 1; pos <= blobColumns.Count; pos++)
												{
													NpgsqlParameter param = new NpgsqlParameter($":bytesData{pos}", NpgsqlDbType.Bytea)
													                        {
														                        Value = reader.GetValue(pos)
													                        };
													updateCommand.Parameters.Add(param);
												}

												updateCommand.ExecuteNonQuery();
												if (++count%1000 == 0)
												{
													Logger.Info($"{count} records updated in table '{mapping.PostgreSqlTableName}'");
												}
											}
										}
										Logger.Info($"{count} records updated in table '{mapping.PostgreSqlTableName}'");
									}
								}
							}

							#endregion
						}
					}

					#endregion

					#region Restore Postgres constraints

					Logger.Info("Add foreign key constraints in PostgreSQL");
					foreach (var query in addConstraintQueries)
					{
						using (IDbCommand addConstraintCommand = postgreSqlConnection.CreateCommand())
						{
							Logger.Debug(query);
							addConstraintCommand.CommandText = query;
							addConstraintCommand.CommandTimeout = 60;
							addConstraintCommand.ExecuteReader().Dispose();
						}
					}

					#endregion
				}
				finally
				{
					mySqlConnection.Close();
					postgreSqlConnection.Close();
				}
			}
		}

		public static IDictionary<string, IList<ColumnDefinition>> GetColumnDefinitionPerTables(IDbConnection connection,
			IList<string> tableNames, string query, Func<string, Type> convertToType)
		{
			IDictionary<string, IList<ColumnDefinition>> columnDefinitions = new Dictionary<string, IList<ColumnDefinition>>();
			using (IDbCommand command = connection.CreateCommand())
			{
				command.CommandText = query;
				command.CommandTimeout = 300;
				using (IDataReader reader = command.ExecuteReader())
				{
					while (reader.Read())
					{
						string tableName = reader.GetString(0);
						if (!tableNames.Contains(tableName))
						{
							throw new Exception($"Unknow table '{tableName}' when retrieving columns definition");
						}
						if (!columnDefinitions.ContainsKey(tableName))
						{
							columnDefinitions.Add(tableName, new List<ColumnDefinition>());
						}
						string dataType = reader.GetString(2);
						columnDefinitions[tableName].Add(new ColumnDefinition
						                                 {
							                                 Name = reader.GetString(1),
							                                 DataType = dataType,
							                                 IsNullable = reader.GetString(3) == "YES",
//							                                 Precision = reader.IsDBNull(4) ? (double?) null : reader.GetDouble(4), TODO: useful?
							                                 Position = reader.GetInt32(5),
							                                 Type = convertToType(dataType)
						                                 });
					}
				}
			}
			return columnDefinitions;
		}

		public static IList<string> GetTableNames(IDbConnection connection, string query)
		{
			IList<string> sqlTableNames = new List<string>();
			using (IDbCommand command = connection.CreateCommand())
			{
				command.CommandText = query;
				command.CommandTimeout = 30;
				using (IDataReader reader = command.ExecuteReader())
				{
					while (reader.Read())
					{
						sqlTableNames.Add(reader.GetString(0));
					}
				}
			}
			return sqlTableNames;
		}

		public static ColumnDefinition FindEquivalentColumn(ColumnDefinition fromColumn, IList<ColumnDefinition> toColumns)
		{
			ColumnDefinition toColumn = toColumns.SingleOrDefault(x => x.Position == fromColumn.Position);
			if (toColumn != null)
			{
				if (!fromColumn.Name.Replace("_", string.Empty).EqualsInvariantIgnoreCase(toColumn.Name.Replace("_", string.Empty)))
				{
					toColumn = null;
				}
			}
			return toColumn ??
			       (toColumns.SingleOrDefault(
				       x => x.Name.Replace("_", string.Empty).EqualsInvariantIgnoreCase(fromColumn.Name.Replace("_", string.Empty))));
		}

		public static void Serialize(NpgsqlCopySerializer serializer, IDataReader reader, IDictionary<ColumnDefinition, ColumnDefinition> mappingColumns)
		{
			// NOTE: order by destination table (if order of columns is different between 2 sources)
			foreach (var mapping in mappingColumns.OrderBy(x => x.Value.Position))
			{
				ColumnDefinition mySqlColumn = mapping.Key;
				ColumnDefinition postgreSqlColumn = mapping.Value;
				object value = MySqlHelper.GetValueFromDataReader(reader, mySqlColumn);

				if (mySqlColumn.Type != postgreSqlColumn.Type)
				{
					value = Mapping.ConversionTable[mySqlColumn.Type][postgreSqlColumn.Type](value);
				}

				PostgreSqlHelper.AddToSerializer(serializer, postgreSqlColumn, value);
			}
			serializer.EndRow();
			serializer.Flush();
		}

		public static string GetAppSettingValue(string name, string keyPrefix)
		{
			string value = string.Empty;
			if (!string.IsNullOrWhiteSpace(keyPrefix))
			{
				value = ConfigurationManager.AppSettings[keyPrefix + ":" + name];
			}
			if (string.IsNullOrEmpty(value))
			{
				value = ConfigurationManager.AppSettings[name];
			}
			return value == null ? null : Environment.ExpandEnvironmentVariables(value);
		}
	}
}
