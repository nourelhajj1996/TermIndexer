using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace TermIndexer
{
    public class FilesReader
    {

        //load all text files from a directory
        public List<string> LoadFiles(string directory)
        {
            List<string> filesNames = new List<string>();

            DirectoryInfo dirInfo = new DirectoryInfo(directory);
            FileInfo[] files = dirInfo.GetFiles("*.txt");

            foreach (FileInfo file in files)
            {
                filesNames.Add(file.Name);
            }

            return filesNames;
        }

        //Read the file line by line
        public List<string> ReadFile(string filePath)
        {
            string line;
            List<string> lines = new List<string>();
            StreamReader file = new StreamReader(filePath);

            while ((line = file.ReadLine()) != null)
            {
                lines.Add(line);
            }

            return lines;
        }

        //Split line into words only
        public List<string> SplitLineToWords(string line)
        {
            List<string> words = new List<string>();

            string[] substrings = Regex.Split(line, @"\W");

            foreach (string word in substrings)
            {
                words.Add(word);
            }

            return words;
        }

        //Get non-duplicates
        //public List<string> GetNonDuplicates(List<string> words)
        //{
        //    List<string> nonDuplicates = new List<string>();

        //    foreach(string word in words)
        //    {
        //        if (!nonDuplicates.Contains(word.ToLower()))
        //            nonDuplicates.Add(word); 
        //    }

        //    return nonDuplicates;
        //}
    }
}
