using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TermIndexer
{
    public class Program
    {
        private static FilesReader fr = new FilesReader();
        private static Repository rep = new Repository();
        private static ConcurrentBag<string> nonDuplicateWords;
        private static Task stage1, stage2, stage3, stage4, stage5;

        public static void Main(string[] args)
        {
            CancellationToken token = new CancellationToken();
            //if (rep.getAllWordsFromDB().Any())
                nonDuplicateWords = new ConcurrentBag<string>(rep.getAllWordsFromDB());
            //else
            //    nonDuplicateWords = new ConcurrentBag<string>();
            DoPipeline(token);
        }

        public static void DoPipeline(CancellationToken token)
        {
            int BufferSize = 32;
            var buffer1 = new BlockingCollection<string>(BufferSize);
            var buffer2 = new BlockingCollection<string>(BufferSize);
            var buffer3 = new BlockingCollection<string>(BufferSize);
            var buffer4 = new BlockingCollection<string>(BufferSize);

            using (CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(token))
            {
                var f = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.None);

                stage1 = f.StartNew(() => DoLoadFiles(@"C:\Test\", buffer1, cts));
                stage2 = f.StartNew(() => DoReadFile(buffer1, buffer2, cts));
                stage3 = f.StartNew(() => DoSplitLinesToWords(buffer2, buffer3, cts));
                stage4 = f.StartNew(() => DoRemoveDuplicate(buffer3, buffer4, cts));
                stage5 = f.StartNew(() => DoInsertWordsToDB(buffer4, cts));
            }
            Task.WaitAll(stage1, stage2, stage3, stage4, stage5);

        }

        public static void DoLoadFiles(string directory, BlockingCollection<string> output, CancellationTokenSource cts)
        {
            try
            {
                var token = cts.Token;

                foreach (string fileName in fr.LoadFiles(directory))
                {
                    if (token.IsCancellationRequested)
                        break;

                    output.Add(directory + fileName, token);
                }
            }
            catch (Exception e)
            {
                // If an exception occurs, notify all other pipeline stages.
                cts.Cancel();
                if (!(e is OperationCanceledException))
                    throw;
            }
            finally
            {
                output.CompleteAdding();
            }
        }

        public static void DoReadFile(BlockingCollection<string> input, BlockingCollection<string> output, CancellationTokenSource cts)
        {
            try
            {
                var token = cts.Token;

                foreach (string directory in input.GetConsumingEnumerable())
                {
                    if (token.IsCancellationRequested)
                        break;

                    foreach (string line in fr.ReadFile(directory))
                    {
                        output.Add(line, token);
                    }
                }
            }
            catch (Exception e)
            {
                // If an exception occurs, notify all other pipeline stages.
                cts.Cancel();
                if (!(e is OperationCanceledException))
                    throw;
            }
            finally
            {
                output.CompleteAdding();
            }
        }

        public static void DoSplitLinesToWords(BlockingCollection<string> input, BlockingCollection<string> output, CancellationTokenSource cts)
        {
            try
            {
                var token = cts.Token;

                foreach (string line in input.GetConsumingEnumerable())
                {
                    if (token.IsCancellationRequested)
                        break;

                    foreach (string word in fr.SplitLineToWords(line))
                    {
                        output.Add(word.ToLower(), token);
                    }
                }
            }
            catch (Exception e)
            {
                // If an exception occurs, notify all other pipeline stages.
                cts.Cancel();
                if (!(e is OperationCanceledException))
                    throw;
            }
            finally
            {
                output.CompleteAdding();
            }
        }

        public static void DoRemoveDuplicate(BlockingCollection<string> input, BlockingCollection<string> output, CancellationTokenSource cts)
        {
            try
            {
                var token = cts.Token;

                foreach (string word in input.GetConsumingEnumerable())
                {
                    if (token.IsCancellationRequested)
                        break;

                    if (!nonDuplicateWords.Contains(word))
                    {
                        nonDuplicateWords.Add(word);
                        output.Add(word, token);
                    }                  
                }
            }
            catch (Exception e)
            {
                // If an exception occurs, notify all other pipeline stages.
                cts.Cancel();
                if (!(e is OperationCanceledException))
                    throw;
            }
            finally
            {
                output.CompleteAdding();
            }
        }

        public static void DoInsertWordsToDB(BlockingCollection<string> input, CancellationTokenSource cts)
        {
            var token = cts.Token;

            foreach (string word in input.GetConsumingEnumerable())
            {
                if (token.IsCancellationRequested)
                    break;

                rep.InsertWordsToDB(word);
            }                
        }

    }
}
