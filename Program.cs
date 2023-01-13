using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.File;
using System;
using System.IO;
using System.Threading.Tasks;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Net;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Security.Authentication;
using System.Security.Cryptography;
using Microsoft.WindowsAzure.Storage.Auth;

namespace SyncFiles
{
    class Program
    {
        static string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=memoriesanonima;AccountKey={key};EndpointSuffix=core.windows.net";
 
        static string shareReference = "memories";
        //static string shareReference = "Documents";
        
        private static readonly OperationContext oc = new OperationContext();

        static void Main(string[] args)
        {
            var pars = args.ToKeyValues();
            Console.WriteLine(pars["-p"]);

            _startDir = pars["-p"];

            MainSendToCloud(_startDir, _startDir+pars["-s"]);
        }

        static void MainSendToCloud(string path, string filter)
        {
            CloudFileDirectory root = GetRoot("Pictures");

            OperationContext.GlobalRequestCompleted += new EventHandler<RequestEventArgs>((req, e) =>
            {
                var r = req as Microsoft.WindowsAzure.Storage.OperationContext;
                Console.WriteLine(" " + r.LastResult.EndTime.ToString() + "  " + r.LastResult.ContentMd5 + "  " + e.RequestUri.LocalPath);
            });
            var f = Directories(path, filter, root).Result;
        }

        public static CloudFileDirectory GetRoot(string sub)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            CloudFileClient fileClient = storageAccount.CreateCloudFileClient();
            CloudFileShare share = fileClient.GetShareReference(shareReference.ToLower());
            CloudFileDirectory root = share.GetRootDirectoryReference();
            // CloudFileDirectory cloudPictures = root.GetDirectoryReference("Pictures");
            CloudFileDirectory cloudPictures = root.GetDirectoryReference(sub);
            return cloudPictures;
        }

        private static string _startDir;

        public static async Task<string> Directories(string startDir, string filter, CloudFileDirectory dirBase)
        {
            foreach (var dir in Directory.GetDirectories(startDir))
            {
                try
                {
                    var ff = Path.GetFileName(dir);
                    CloudFileDirectory dirCloud = dirBase.GetDirectoryReference(ff);
                    if (!dir.StartsWith(filter))
                    {
                        await Directories(dir, filter, dirCloud);
                        continue;
                    }
                    await dirCloud.CreateIfNotExistsAsync();
                    var list = await dirCloud.ListFilesAndDirectoriesSegmentedAsync(int.MaxValue, new FileContinuationToken(), new FileRequestOptions() { StoreFileContentMD5 = true }, oc);
                    //var list = await dirCloud.ListFilesAndDirectoriesSegmentedAsync(new FileContinuationToken());
                    foreach (var fileName in Directory.GetFiles(dir))
                    {
                        //Console.WriteLine(DateTime.Now.ToString() + " CHECK " + fileName);
                        FileInfo file = new FileInfo(fileName);
                        var blobFile = list.Results.OfType<CloudFile>().FirstOrDefault(s => s.Name == file.Name);
                        if (blobFile != null)
                        {
                            await blobFile.FetchAttributesAsync();
                        }
                        if (blobFile == null || blobFile.Properties.ContentMD5 == null || file.Length != blobFile.Properties.Length)
                        {
                            Console.WriteLine(DateTime.Now.ToString() + " START " + file.Name);
                            var f = await UploadFile(file, dirCloud);
                        }
                    }
                    await Directories(dir, filter, dirCloud);
                }
                catch (Exception xx)
                {
                    Console.WriteLine($"FAIL {xx.Message}");
                }
            }
            return "ok";
        }

        public static async Task<string> UploadFile(FileInfo file, CloudFileDirectory dirCloud)
        {
            try
            {
                CloudFile cloudFile = dirCloud.GetFileReference(file.Name);
                using (FileStream fs = file.OpenRead())
                {
                    if(file.Extension.ToUpper() == ".CR2")
                        cloudFile.Properties.ContentType = "image/x-canon-cr2";
                    if(file.Extension.ToUpper() == ".MP4")
                        cloudFile.Properties.ContentType = "video/mp4";
                    if(file.Extension.ToUpper() == ".JPEG")
                        cloudFile.Properties.ContentType = "image/jpeg";
                    if(file.Extension.ToUpper() == ".JPG")
                        cloudFile.Properties.ContentType = "image/jpeg";
                    await cloudFile.UploadFromStreamAsync(fs, AccessCondition.GenerateEmptyCondition(), new FileRequestOptions() { StoreFileContentMD5 = true }, oc);
                }
                return file.FullName;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"retry {file.FullName}, {ex.Message}");
                if (ex.Message != "The specified resource is read-only and cannot be modified at this time.")
                {
                    return await UploadFile(file, dirCloud);
                }
                return "KO";
            }
        }
    }

    public static class MyLoDash
    {
        public static Dictionary<string, string> ToKeyValues(this string[] array)
        {
            var length = array.Length - array.Length % 2;
            var res = new Dictionary<string, string>();
            for (int i = 0; i <= length / 2; i = i + 2)
            {
                res.Add(array[i], array[i + 1]);
            }
            if (array.Length % 2 == 1)
            {
                res.Add(array[array.Length - 1], "");
            }
            return res;
        }
    }
}
