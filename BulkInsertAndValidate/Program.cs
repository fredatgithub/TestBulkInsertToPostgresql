using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using Npgsql;

namespace BulkInsertAndValidate
{
  internal static class Program
  {
    static void Main()
    {
      const string connectionString = "Host=localhost;Port=5432;Username=username;Password=motdepasse;Database=ta_base";
      const string fichierCsv = @"C:\chemin\fichier.csv";
      const string fichierTempValide = @"C:\chemin\temp_valide.csv";
      const string fichierErreurs = @"C:\chemin\erreurs.csv";

      // Lire et valider le CSV
      var lignesValides = new List<string>();
      var lignesInvalides = new List<string>();
      char separateur = DetectSeparator(File.ReadLines(fichierCsv).First());

      int ligneIndex = 0;
      foreach (var ligne in File.ReadLines(fichierCsv))
      {
        ligneIndex++;

        // Garder l'en-tête
        if (ligneIndex == 1)
        {
          lignesValides.Add(ligne);
          continue;
        }

        string[] colonnes = ligne.Split(separateur);

        // Validation basique : 3 colonnes et id est un int
        if (colonnes.Length != 3 || !int.TryParse(colonnes[0], out _))
        {
          lignesInvalides.Add($"Ligne {ligneIndex}: {ligne}");
        }
        else
        {
          lignesValides.Add(ligne);
        }
      }

      // Sauvegarder les erreurs
      if (lignesInvalides.Any())
      {
        File.WriteAllLines(fichierErreurs, lignesInvalides);
        Console.WriteLine($"⚠️ {lignesInvalides.Count} ligne(s) invalide(s) enregistrée(s) dans : {fichierErreurs}");
      }

      // Sauvegarder les lignes valides dans un fichier temporaire
      File.WriteAllLines(fichierTempValide, lignesValides);

      // Bulk insert via COPY
      string commandeCopy = $"COPY users (id, name, email) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER '{separateur}')";

      try
      {
        using (var conn = new NpgsqlConnection(connectionString))
        {
          conn.Open();

          using (var writer = conn.BeginTextImport(commandeCopy))
          using (var reader = new StreamReader(fichierTempValide))
          {
            while (!reader.EndOfStream)
              writer.WriteLine(reader.ReadLine());
          }

          Console.WriteLine("✅ Insertion terminée.");
        }
      }
      catch (Exception exception)
      {
        Console.WriteLine("❌ Erreur lors de l'insertion : " + exception.Message);
      }
    }

    static char DetectSeparator(string ligne)
    {
      char[] sepPossibles = new[] { ';', ',', '\t', '|' };
      return sepPossibles
          .Select(sep => new { sep, count = ligne.Count(c => c == sep) })
          .OrderByDescending(x => x.count)
          .First().sep;
    }
  }
}
