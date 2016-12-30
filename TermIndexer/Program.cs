using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TermIndexer
{
    public class Program
    {
        static string directory = "";
        static FilesReader fr = new FilesReader();
        static Repository rep = new Repository();
        static ConcurrentBag<string> nonDuplicateWords = new ConcurrentBag<string>(); 

        public static void Main(string[] args)
        {
            int BufferSize = 32;
            var buffer1 = new BlockingCollection<string>(BufferSize);
            var buffer2 = new BlockingCollection<string>(BufferSize);
            var buffer3 = new BlockingCollection<string>(BufferSize);

            var f = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.None);

            var stage1 = f.StartNew(() => DoReadFile(buffer1));
            var stage2 = f.StartNew(() => DoSplitLinesToWords(buffer1, buffer2));
            var stage3 = f.StartNew(() => DoRemoveDuplicate(buffer2, buffer3));
            var stage4 = f.StartNew(() => DoInsertWordsToDB(buffer3));

            Task.WaitAll(stage1, stage2, stage3, stage4);
        }

        //public static void DoLoadFiles(BlockingCollection<string> output, string directory)
        //{
        //    try
        //    {
        //        foreach(string fileName in fr.LoadFiles(directory))
        //        {
        //            output.Add(fileName);
        //        }
        //    }
        //    finally
        //    {
        //        output.CompleteAdding();
        //    }
        //}

        public static void DoReadFile(BlockingCollection<string> output)
        {
            try
            {
                foreach (string line in fr.ReadFile(directory))
                {
                    output.Add(line);
                }
            }
            finally
            {
                output.CompleteAdding();
            }
        }

        public static void DoSplitLinesToWords(BlockingCollection<string> input, BlockingCollection<string> output)
        {
            try
            {
                foreach (string line in input.GetConsumingEnumerable())
                {
                    foreach (string word in fr.SplitLineToWords(line))
                    {
                        output.Add(word);
                    }
                }
            }
            finally
            {
                output.CompleteAdding();
            }
        }

        public static void DoRemoveDuplicate(BlockingCollection<string> input, BlockingCollection<string> output)
        {
            try
            {
                foreach (string word in input.GetConsumingEnumerable())
                {
                    string w = word;

                    if (!nonDuplicateWords.TryPeek(out w))
                    {
                        nonDuplicateWords.Add(word);
                        output.Add(word);
                    }                  
                }
            }
            finally
            {
                output.CompleteAdding();
            }
        }

        public static void DoInsertWordsToDB(BlockingCollection<string> input)
        {
            foreach (string word in input.GetConsumingEnumerable())
            {
                rep.InsertWordsToDB(word);
            }                
        }

    }
}
