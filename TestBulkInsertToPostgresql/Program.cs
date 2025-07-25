using System;
using System.IO;
using Npgsql;

namespace TestBulkInsertToPostgresql
{
  internal static class Program
  {
    static void Main()
    {
      Console.WriteLine("This is a test for bulk insert to PostgreSQL.");
      const string connectionString = "Host=localhost;Port=5432;Username=username;Password=password;Database=mydatabase";
      const string filePath = "test.csv";
      try
      {
        using (var conn = new NpgsqlConnection(connectionString))
        {
          conn.Open();

          using (var writer = conn.BeginTextImport("COPY tableName (col1,col2,col3) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ';')"))
          using (var reader = new StreamReader(filePath))
          {
            while (!reader.EndOfStream)
            {
              var line = reader.ReadLine();
              writer.WriteLine(line);
              Console.WriteLine($"Inserted line: {line}");
            }
          }

          Console.WriteLine("Bulk insert terminé.");
        }
      }
      catch (Exception exception)
      {
        Console.WriteLine("An error occurred during the bulk insert operation.");
        Console.WriteLine($"the error is: {exception.Message}");
      }

      Console.WriteLine("Press any key to exit...");
      Console.ReadKey();
    }
  }
}
