using System;
using System.IO;
using System.Globalization;
using System.Text;
using System.Collections.Generic;

public class Program
{
    public static void Main(string[] args)
    {
        string path = @"C:\\Users\\joaov\\source\\repos\\ArquivosComIndicesParciais\\ArquivosComIndicesParciais\\2019-Nov.csv";
        string produtosBinFilePath = @"..\..\produtos.bin";
        string produtoIndexFilePath = @"..\..\produto_index.idx";
        string acessosBinFilePath = @"..\..\acessos.bin";
        string acessoIndexFilePath = @"..\..\acesso_index.idx";

        while (true)
        {
            Console.Clear();
            Console.WriteLine("Menu:");
            Console.WriteLine("1. Criar arquivos binários e índices");
            Console.WriteLine("2. Acessar o arquivo 'produtos.bin'");
            Console.WriteLine("3. Acessar o arquivo 'acessos.bin'");
            Console.WriteLine("4. Sair");
            Console.Write("Escolha uma opção: ");

            string choice = Console.ReadLine();
            switch (choice)
            {
                case "1":
                    CreateBinaryFilesAndIndices(path, produtosBinFilePath, produtoIndexFilePath, acessosBinFilePath, acessoIndexFilePath);
                    break;
                case "2":
                    ReadProdutosBin(produtosBinFilePath, produtoIndexFilePath);
                    break;
                case "3":
                    ReadAcessosBin(acessosBinFilePath, acessoIndexFilePath);
                    break;
                case "4":
                    return;
                default:
                    Console.WriteLine("Opção inválida. Pressione Enter para continuar.");
                    Console.ReadLine();
                    break;
            }
        }
    }

    public static void CreateBinaryFilesAndIndices(string csvFilePath, string produtosBinFilePath, string produtoIndexFilePath, string acessosBinFilePath, string acessoIndexFilePath)
    {
        CreateBinaryFiles(csvFilePath, produtosBinFilePath, acessosBinFilePath);

        ReconstructProdutoDataAndIndex(produtosBinFilePath, produtoIndexFilePath);
        ReconstructAcessoDataAndIndex(acessosBinFilePath, acessoIndexFilePath);

        Console.WriteLine("Arquivos binários e índices criados com sucesso. Pressione Enter para continuar.");
        Console.ReadLine();
    }

    public static void CreateBinaryFiles(string csvFilePath, string produtosBinFilePath, string acessosBinFilePath)
    {
        int chunkSize = 100000; // registros por bloco
        List<string> tempProdutoFiles = new List<string>();
        List<string> tempAcessoFiles = new List<string>();

        using (var reader = new StreamReader(csvFilePath))
        {
            // Pular o cabeçalho
            string headerLine = reader.ReadLine();

            bool endOfFile = false;
            int chunkIndex = 0;

            while (!endOfFile)
            {
                List<Produto> produtosChunk = new List<Produto>();
                List<Acesso> acessosChunk = new List<Acesso>();

                for (int i = 0; i < chunkSize; i++)
                {
                    if (reader.EndOfStream)
                    {
                        endOfFile = true;
                        break;
                    }

                    var line = reader.ReadLine();
                    var values = line.Split(',');

                    // Verificar se a linha tem o número esperado de colunas
                    if (values.Length < 9)
                    {
                        Console.WriteLine($"Linha inválida encontrada e ignorada: {line}");
                        continue;
                    }

                    try
                    {
                        // Extrair os campos necessários
                        string timestamp = values[0];
                        string eventType = values[1];
                        long productId = long.Parse(values[2]);
                        string categoryId = values[3];
                        string categoryCode = values[4];
                        string brand = values[5];
                        double price = double.Parse(values[6], CultureInfo.InvariantCulture);
                        long userId = long.Parse(values[7]);
                        string sessionId = values[8];

                        // Criar Produto
                        Produto produto = new Produto
                        {
                            ProductID = productId,
                            Preco = price,
                            Categoria = categoryCode,
                            Deleted = false
                        };

                        produtosChunk.Add(produto);

                        // Criar Acesso
                        DateTime eventTime = DateTime.ParseExact(timestamp, "yyyy-MM-dd HH:mm:ss 'UTC'", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);

                        Acesso acesso = new Acesso
                        {
                            UserID = userId,
                            EventType = eventType,
                            EventTime = eventTime,
                            Deleted = false
                        };

                        acessosChunk.Add(acesso);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Erro ao processar a linha: {line}");
                        Console.WriteLine($"Detalhes do erro: {ex.Message}");
                        continue;
                    }
                }

                // Ordenar os blocos
                produtosChunk.Sort((p1, p2) => p1.ProductID.CompareTo(p2.ProductID));
                acessosChunk.Sort((a1, a2) => a1.UserID.CompareTo(a2.UserID));

                // arquivos temporários
                string tempProdutoFile = $"temp_produtos_{chunkIndex}.bin";
                using (var produtosBinWriter = new BinaryWriter(File.Open(tempProdutoFile, FileMode.Create)))
                {
                    foreach (var produto in produtosChunk)
                    {
                        byte[] produtoBytes = produto.ToBytes();
                        produtosBinWriter.Write(produtoBytes);
                    }
                }
                tempProdutoFiles.Add(tempProdutoFile);

                string tempAcessoFile = $"temp_acessos_{chunkIndex}.bin";
                using (var acessosBinWriter = new BinaryWriter(File.Open(tempAcessoFile, FileMode.Create)))
                {
                    foreach (var acesso in acessosChunk)
                    {
                        byte[] acessoBytes = acesso.ToBytes();
                        acessosBinWriter.Write(acessoBytes);
                    }
                }
                tempAcessoFiles.Add(tempAcessoFile);

                chunkIndex++;
            }
        }

        // Intercalar arquivos temporários para criar arquivos finais ordenados
        MergeSortedFiles(tempProdutoFiles, produtosBinFilePath, Produto.TamanhoRegistro, CompareProdutos);
        MergeSortedFiles(tempAcessoFiles, acessosBinFilePath, Acesso.TamanhoRegistro, CompareAcessos);

        // Remover arquivos temporários
        foreach (var tempFile in tempProdutoFiles)
            File.Delete(tempFile);

        foreach (var tempFile in tempAcessoFiles)
            File.Delete(tempFile);

        Console.WriteLine($"Dados de produtos foram salvos em {produtosBinFilePath}");
        Console.WriteLine($"Dados de acessos foram salvos em {acessosBinFilePath}");
    }

    public static void MergeSortedFiles(List<string> inputFiles, string outputFile, int recordSize, Comparison<byte[]> compareRecords)
    {
        List<BinaryReader> readers = new List<BinaryReader>();
        foreach (var file in inputFiles)
        {
            readers.Add(new BinaryReader(File.Open(file, FileMode.Open, FileAccess.Read)));
        }

        using (var writer = new BinaryWriter(File.Open(outputFile, FileMode.Create)))
        {
            SortedSet<HeapItem> heap = new SortedSet<HeapItem>(new HeapItemComparer(compareRecords));
            byte[][] buffers = new byte[readers.Count][];

            // primeiro registro de cada arquivo
            for (int i = 0; i < readers.Count; i++)
            {
                if (readers[i].BaseStream.Position < readers[i].BaseStream.Length)
                {
                    buffers[i] = readers[i].ReadBytes(recordSize);
                    heap.Add(new HeapItem(buffers[i], i));
                }
            }

            // Intercalar
            while (heap.Count > 0)
            {
                // menor registro 
                var minItem = heap.Min;
                heap.Remove(minItem);

                byte[] minRecord = minItem.Record;
                int readerIndex = minItem.SourceIndex;

                // Escreve no arquivo 
                writer.Write(minRecord);

                // Le o próximo registro
                if (readers[readerIndex].BaseStream.Position < readers[readerIndex].BaseStream.Length)
                {
                    buffers[readerIndex] = readers[readerIndex].ReadBytes(recordSize);
                    heap.Add(new HeapItem(buffers[readerIndex], readerIndex));
                }
            }
        }

        foreach (var reader in readers)
            reader.Close();
    }



    public static int CompareProdutos(byte[] x, byte[] y)
    {
        long productIdX = BitConverter.ToInt64(x, 0);
        long productIdY = BitConverter.ToInt64(y, 0);
        return productIdX.CompareTo(productIdY);
    }

    public static int CompareAcessos(byte[] x, byte[] y)
    {
        long userIdX = BitConverter.ToInt64(x, 0);
        long userIdY = BitConverter.ToInt64(y, 0);
        return userIdX.CompareTo(userIdY);
    }

    public class HeapItem
    {
        public byte[] Record { get; set; }
        public int SourceIndex { get; set; }

        public HeapItem(byte[] record, int sourceIndex)
        {
            Record = record;
            SourceIndex = sourceIndex;
        }
    }

    public class HeapItemComparer : IComparer<HeapItem>
    {
        private Comparison<byte[]> _recordComparison;

        public HeapItemComparer(Comparison<byte[]> recordComparison)
        {
            _recordComparison = recordComparison;
        }

        public int Compare(HeapItem x, HeapItem y)
        {
            int cmp = _recordComparison(x.Record, y.Record);
            if (cmp != 0)
                return cmp;

            // Se os registros são iguais, compara o índice
            return x.SourceIndex.CompareTo(y.SourceIndex);
        }
    }

    public static List<AcessoIndexEntry> LoadAcessoIndex(string acessoIndexFilePath)
    {
        List<AcessoIndexEntry> indexEntries = new List<AcessoIndexEntry>();

        if (!File.Exists(acessoIndexFilePath))
            return indexEntries;

        using (var fs = new FileStream(acessoIndexFilePath, FileMode.Open, FileAccess.Read))
        using (var br = new BinaryReader(fs))
        {
            while (fs.Position < fs.Length)
            {
                long userId = br.ReadInt64();
                long offset = br.ReadInt64();

                AcessoIndexEntry entry = new AcessoIndexEntry
                {
                    UserID = userId,
                    Offset = offset
                };

                indexEntries.Add(entry);
            }
        }

        return indexEntries;
    }

    public static void ReconstructProdutoDataAndIndex(string produtosBinFilePath, string produtoIndexFilePath)
    {
        string tempProdutosBinFilePath = produtosBinFilePath + ".tmp";
        string tempProdutoIndexFilePath = produtoIndexFilePath + ".tmp";

        using (var inputFs = new FileStream(produtosBinFilePath, FileMode.Open, FileAccess.Read))
        using (var outputFs = new FileStream(tempProdutosBinFilePath, FileMode.Create, FileAccess.Write))
        using (var indexFs = new FileStream(tempProdutoIndexFilePath, FileMode.Create, FileAccess.Write))
        using (var indexBw = new BinaryWriter(indexFs))
        {
            //long offset = 0;
            long newOffset = 0;

            byte[] record = new byte[Produto.TamanhoRegistro];

            while (inputFs.Position < inputFs.Length)
            {
                int bytesRead = inputFs.Read(record, 0, Produto.TamanhoRegistro);
                if (bytesRead < Produto.TamanhoRegistro)
                {
                    Console.WriteLine("Tamanho do registro diverge");
                    break;
                }

                Produto p = Produto.FromBytes(record);

                if (!p.Deleted)
                {
                    // Escreve o registro no novo arquivo
                    outputFs.Write(record, 0, Produto.TamanhoRegistro);

                    indexBw.Write(p.ProductID);
                    indexBw.Write(newOffset);

                    newOffset += Produto.TamanhoRegistro;
                }

                //offset += Produto.TamanhoRegistro;
            }
        }

        // Substituir os arquivos antigos pelos novos
        File.Delete(produtosBinFilePath);
        File.Move(tempProdutosBinFilePath, produtosBinFilePath);

        File.Delete(produtoIndexFilePath);
        File.Move(tempProdutoIndexFilePath, produtoIndexFilePath);

        Console.WriteLine("Reconstrução completa. Arquivos de dados e índice atualizados.");
    }

    public static void ReadProdutosBin(string produtosBinFilePath, string produtoIndexFilePath)
    {
        while (true)
        {
            Console.Clear();
            Console.WriteLine("Menu do Arquivo 'produtos.bin':");
            Console.WriteLine("1. Consultar por ProductID");
            Console.WriteLine("2. Ler todo o arquivo");
            Console.WriteLine("3. Adicionar produto");
            Console.WriteLine("4. Remover produto");
            Console.WriteLine("5. Reconstruir índice de produtos");
            Console.WriteLine("6. Voltar ao menu anterior");
            Console.Write("Escolha uma opção: ");

            string choice = Console.ReadLine();
            switch (choice)
            {
                case "1":
                    ConsultarProdutoPorID(produtosBinFilePath, produtoIndexFilePath);
                    break;
                case "2":
                    LerTodosProdutos(produtosBinFilePath);
                    break;
                case "3":
                    AddProduto(produtosBinFilePath, produtoIndexFilePath);
                    break;
                case "4":
                    RemoveProduto(produtosBinFilePath, produtoIndexFilePath);
                    break;
                case "5":
                    ReconstructProdutoDataAndIndex(produtosBinFilePath, produtoIndexFilePath);
                    Console.WriteLine("Índice reconstruído com sucesso. Pressione Enter para continuar.");
                    Console.ReadLine();
                    break;
                case "6":
                    return;
                default:
                    Console.WriteLine("Opção inválida. Pressione Enter para continuar.");
                    Console.ReadLine();
                    break;
            }
        }
    }

    public static void ConsultarProdutoPorID(string produtosBinFilePath, string produtoIndexFilePath)
    {
        Console.Write("Digite o ProductID para consulta: ");
        string input = Console.ReadLine();
        if (long.TryParse(input, out long productId))
        {
            Produto produto = BuscarProdutoPorID(produtosBinFilePath, produtoIndexFilePath, productId);
            if (produto != null)
            {
                Console.WriteLine($"ProductID: {produto.ProductID}, Preço: {produto.Preco}, Categoria: {produto.Categoria}");
            }
            else
            {
                Console.WriteLine("Produto não encontrado.");
            }
        }
        else
        {
            Console.WriteLine("ProductID inválido.");
        }
        Console.WriteLine("Pressione Enter para voltar ao menu anterior.");
        Console.ReadLine();
    }

    public static Produto BuscarProdutoPorID(string produtosBinFilePath, string produtoIndexFilePath, long productId)
    {
        using (var fs = new FileStream(produtoIndexFilePath, FileMode.Open, FileAccess.Read))
        using (var br = new BinaryReader(fs))
        {
            long entrySize = sizeof(long) + sizeof(long); // ProductID + Offset
            long left = 0;
            long right = fs.Length / entrySize - 1;

            while (left <= right)
            {
                long mid = (left + right) / 2;
                fs.Seek(mid * entrySize, SeekOrigin.Begin);
                long key = br.ReadInt64();
                long offset = br.ReadInt64();

                if (key == productId)
                {
                    using (var dataFs = new FileStream(produtosBinFilePath, FileMode.Open, FileAccess.Read))
                    {
                        dataFs.Seek(offset, SeekOrigin.Begin);
                        byte[] record = new byte[Produto.TamanhoRegistro];
                        dataFs.Read(record, 0, Produto.TamanhoRegistro);
                        Produto p = Produto.FromBytes(record);
                        if (!p.Deleted)
                            return p;
                        else
                            return null; // Registro marcado como deletado
                    }
                }
                else if (key < productId)
                {
                    left = mid + 1;
                }
                else
                {
                    right = mid - 1;
                }
            }
        }
        return null; // Não encontrado
    }

    public static void LerTodosProdutos(string produtosBinFilePath)
    {
        using (var fs = new FileStream(produtosBinFilePath, FileMode.Open, FileAccess.Read))
        using (var br = new BinaryReader(fs))
        {
            while (fs.Position < fs.Length)
            {
                byte[] record = br.ReadBytes(Produto.TamanhoRegistro);
                if (record.Length < Produto.TamanhoRegistro)
                {
                    Console.WriteLine("Registro incompleto encontrado. Ignorando.");
                    break;
                }

                Produto p = Produto.FromBytes(record);
                if (!p.Deleted)
                {
                    Console.WriteLine($"ProductID: {p.ProductID}, Preço: {p.Preco}, Categoria: {p.Categoria}");
                }
            }
        }
        Console.WriteLine("Pressione Enter para voltar ao menu anterior.");
        Console.ReadLine();
    }

    public static void AddProduto(string produtosBinFilePath, string produtoIndexFilePath)
    {
        Console.Write("Digite o ProductID: ");
        long productId = long.Parse(Console.ReadLine());
        Console.Write("Digite o Preço: ");
        double preco = double.Parse(Console.ReadLine());
        Console.Write("Digite a Categoria: ");
        string categoria = Console.ReadLine();

        Produto p = new Produto
        {
            ProductID = productId,
            Preco = preco,
            Categoria = categoria,
            Deleted = false
        };

        long offset;
        using (var fs = new FileStream(produtosBinFilePath, FileMode.Append, FileAccess.Write))
        {
            offset = fs.Position;
            byte[] record = p.ToBytes();
            fs.Write(record, 0, record.Length);
        }

        string tempIndexFilePath = produtoIndexFilePath + ".tmp";
        bool inserted = false;

        using (var inputFs = new FileStream(produtoIndexFilePath, FileMode.OpenOrCreate, FileAccess.Read))
        using (var inputBr = new BinaryReader(inputFs))
        using (var outputFs = new FileStream(tempIndexFilePath, FileMode.Create, FileAccess.Write))
        using (var outputBw = new BinaryWriter(outputFs))
        {
            while (inputFs.Position < inputFs.Length)
            {
                long currentProductId = inputBr.ReadInt64();
                long currentOffset = inputBr.ReadInt64();

                if (!inserted && productId < currentProductId)
                {
                    // Insere o novo registro
                    outputBw.Write(productId);
                    outputBw.Write(offset);
                    inserted = true;
                }

                if (currentProductId == productId)
                {
                    Console.WriteLine("ProductID já existe. Operação abortada.");
                    // Remove o arquivo temporário e retorna
                    outputBw.Close();
                    outputFs.Close();
                    File.Delete(tempIndexFilePath);
                    return;
                }

                outputBw.Write(currentProductId);
                outputBw.Write(currentOffset);
            }

            if (!inserted)
            {
                // Insere no final
                outputBw.Write(productId);
                outputBw.Write(offset);
            }
        }

        // Substitui o arquivo de índice antigo pelo novo
        File.Delete(produtoIndexFilePath);
        File.Move(tempIndexFilePath, produtoIndexFilePath);

        Console.WriteLine("Produto adicionado com sucesso!");
        Console.WriteLine("Pressione Enter para continuar.");
        Console.ReadLine();
    }

    public static void AddAcesso(string acessosBinFilePath, string acessoIndexFilePath)
    {
        Console.Write("Digite o UserID: ");
        long userId = long.Parse(Console.ReadLine());
        Console.Write("Digite o EventType: ");
        string eventType = Console.ReadLine();
        Console.Write("Digite o EventTime (formato yyyy-MM-dd HH:mm:ss): ");
        string eventTimeInput = Console.ReadLine();
        DateTime eventTime;
        if (!DateTime.TryParseExact(eventTimeInput, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out eventTime))
        {
            Console.WriteLine("Formato de data inválido.");
            Console.WriteLine("Pressione Enter para continuar.");
            Console.ReadLine();
            return;
        }

        Acesso a = new Acesso
        {
            UserID = userId,
            EventType = eventType,
            EventTime = eventTime,
            Deleted = false
        };

        long offset;
        using (var fs = new FileStream(acessosBinFilePath, FileMode.Append, FileAccess.Write))
        {
            offset = fs.Position;
            byte[] record = a.ToBytes();
            fs.Write(record, 0, record.Length);
        }

        string tempIndexFilePath = acessoIndexFilePath + ".tmp";
        bool inserted = false;

        using (var inputFs = new FileStream(acessoIndexFilePath, FileMode.OpenOrCreate, FileAccess.Read))
        using (var inputBr = new BinaryReader(inputFs))
        using (var outputFs = new FileStream(tempIndexFilePath, FileMode.Create, FileAccess.Write))
        using (var outputBw = new BinaryWriter(outputFs))
        {
            while (inputFs.Position < inputFs.Length)
            {
                long currentUserId = inputBr.ReadInt64();
                long currentOffset = inputBr.ReadInt64();

                if (!inserted && userId <= currentUserId)
                {
                    // Insere o novo
                    outputBw.Write(userId);
                    outputBw.Write(offset);
                    inserted = true;
                }

                outputBw.Write(currentUserId);
                outputBw.Write(currentOffset);
            }

            if (!inserted)
            {
                // Insere no final
                outputBw.Write(userId);
                outputBw.Write(offset);
            }
        }

        File.Delete(acessoIndexFilePath);
        File.Move(tempIndexFilePath, acessoIndexFilePath);

        Console.WriteLine("Acesso adicionado com sucesso!");
        Console.WriteLine("Pressione Enter para continuar.");
        Console.ReadLine();
    }


    public static void RemoveProduto(string produtosBinFilePath, string produtoIndexFilePath)
    {
        Console.Write("Digite o ProductID para remover: ");
        long productId = long.Parse(Console.ReadLine());

        string tempIndexFilePath = produtoIndexFilePath + ".tmp";
        bool found = false;
        long offsetToDelete = -1;

        using (var inputFs = new FileStream(produtoIndexFilePath, FileMode.Open, FileAccess.Read))
        using (var inputBr = new BinaryReader(inputFs))
        using (var outputFs = new FileStream(tempIndexFilePath, FileMode.Create, FileAccess.Write))
        using (var outputBw = new BinaryWriter(outputFs))
        {
            while (inputFs.Position < inputFs.Length)
            {
                long currentProductId = inputBr.ReadInt64();
                long currentOffset = inputBr.ReadInt64();

                if (currentProductId == productId)
                {
                    found = true;
                    offsetToDelete = currentOffset;
                    // Não escreve o registro no novo índice
                }
                else
                {
                    outputBw.Write(currentProductId);
                    outputBw.Write(currentOffset);
                }
            }
        }

        if (found)
        {
            // Marca como deletado no arquivo de dados
            using (var fs = new FileStream(produtosBinFilePath, FileMode.Open, FileAccess.Write))
            {
                fs.Seek(offsetToDelete + Produto.TamanhoRegistro - 1, SeekOrigin.Begin);
                fs.WriteByte(1); // true
            }

            // Substitui o arquivo de índice antigo pelo novo
            File.Delete(produtoIndexFilePath);
            File.Move(tempIndexFilePath, produtoIndexFilePath);

            Console.WriteLine("Produto removido com sucesso!");
        }
        else
        {
            // Remove o arquivo temporário se o produto não foi encontrado
            File.Delete(tempIndexFilePath);
            Console.WriteLine("Produto não encontrado.");
        }

        Console.WriteLine("Pressione Enter para continuar.");
        Console.ReadLine();
    }

    public static void RemoveAcesso(string acessosBinFilePath, string acessoIndexFilePath)
    {
        Console.Write("Digite o UserID do acesso a ser removido: ");
        if (!long.TryParse(Console.ReadLine(), out long userId))
        {
            Console.WriteLine("UserID inválido.");
            Console.WriteLine("Pressione Enter para continuar.");
            Console.ReadLine();
            return;
        }

        Console.Write("Digite o EventType do acesso a ser removido: ");
        string eventType = Console.ReadLine();

        Console.Write("Digite o EventTime do acesso a ser removido (formato yyyy-MM-dd HH:mm:ss): ");
        if (!DateTime.TryParseExact(Console.ReadLine(), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime eventTime))
        {
            Console.WriteLine("Formato de data inválido.");
            Console.WriteLine("Pressione Enter para continuar.");
            Console.ReadLine();
            return;
        }

        bool found = false;
        long offsetToDelete = -1;
        string tempIndexFilePath = acessoIndexFilePath + ".tmp";

        using (var inputFs = new FileStream(acessoIndexFilePath, FileMode.Open, FileAccess.Read))
        using (var inputBr = new BinaryReader(inputFs))
        using (var outputFs = new FileStream(tempIndexFilePath, FileMode.Create, FileAccess.Write))
        using (var outputBw = new BinaryWriter(outputFs))
        {
            while (inputFs.Position < inputFs.Length)
            {
                long currentUserId = inputBr.ReadInt64();
                long currentOffset = inputBr.ReadInt64();

                if (currentUserId == userId)
                {
                    using (var fs = new FileStream(acessosBinFilePath, FileMode.Open, FileAccess.Read))
                    {
                        fs.Seek(currentOffset, SeekOrigin.Begin);
                        byte[] record = new byte[Acesso.TamanhoRegistro];
                        fs.Read(record, 0, Acesso.TamanhoRegistro);
                        Acesso a = Acesso.FromBytes(record);

                        if (!a.Deleted && a.EventType == eventType && a.EventTime == eventTime)
                        {
                            // Encontrou
                            found = true;
                            offsetToDelete = currentOffset;
                            continue; // Não escrever esta entrada no novo índice
                        }
                    }
                }

                outputBw.Write(currentUserId);
                outputBw.Write(currentOffset);
            }
        }

        if (found)
        {
            using (var fs = new FileStream(acessosBinFilePath, FileMode.Open, FileAccess.Write))
            {
                fs.Seek(offsetToDelete + Acesso.TamanhoRegistro - 1, SeekOrigin.Begin);
                fs.WriteByte(1); // true
            }

            // Substitui o arquivo de índice antigo pelo novo
            File.Delete(acessoIndexFilePath);
            File.Move(tempIndexFilePath, acessoIndexFilePath);

            Console.WriteLine("Acesso removido com sucesso!");
        }
        else
        {
            // Remove o arquivo temporário se o acesso não foi encontrado
            File.Delete(tempIndexFilePath);
            Console.WriteLine("Acesso não encontrado.");
        }

        Console.WriteLine("Pressione Enter para continuar.");
        Console.ReadLine();
    }

    public static void ReconstructAcessoDataAndIndex(string acessosBinFilePath, string acessoIndexFilePath)
    {
        string tempAcessosBinFilePath = acessosBinFilePath + ".tmp";
        string tempAcessoIndexFilePath = acessoIndexFilePath + ".tmp";

        using (var inputFs = new FileStream(acessosBinFilePath, FileMode.Open, FileAccess.Read))
        using (var outputFs = new FileStream(tempAcessosBinFilePath, FileMode.Create, FileAccess.Write))
        using (var indexFs = new FileStream(tempAcessoIndexFilePath, FileMode.Create, FileAccess.Write))
        using (var indexBw = new BinaryWriter(indexFs))
        {
            long offset = 0;
            long newOffset = 0;

            byte[] record = new byte[Acesso.TamanhoRegistro];

            while (inputFs.Position < inputFs.Length)
            {
                int bytesRead = inputFs.Read(record, 0, Acesso.TamanhoRegistro);
                if (bytesRead < Acesso.TamanhoRegistro)
                {
                    Console.WriteLine("Registro incompleto encontrado. Ignorando.");
                    break;
                }

                Acesso a = Acesso.FromBytes(record);

                if (!a.Deleted)
                {
                    // Escreve o registro no novo arquivo de dados
                    outputFs.Write(record, 0, Acesso.TamanhoRegistro);

                    // Escreve no índice com o novo deslocamento
                    indexBw.Write(a.UserID);
                    indexBw.Write(newOffset);

                    newOffset += Acesso.TamanhoRegistro;
                }

                offset += Acesso.TamanhoRegistro;
            }
        }

        // Substituir os arquivos antigos pelos novos
        File.Delete(acessosBinFilePath);
        File.Move(tempAcessosBinFilePath, acessosBinFilePath);

        File.Delete(acessoIndexFilePath);
        File.Move(tempAcessoIndexFilePath, acessoIndexFilePath);

        Console.WriteLine("Reconstrução completa. Arquivos de dados e índice atualizados.");
    }



    public static void ReadAcessosBin(string acessosBinFilePath, string acessoIndexFilePath)
    {
        while (true)
        {
            Console.Clear();
            Console.WriteLine("Menu do Arquivo 'acessos.bin':");
            Console.WriteLine("1. Consultar por UserID");
            Console.WriteLine("2. Ler todo o arquivo");
            Console.WriteLine("3. Adicionar acesso");
            Console.WriteLine("4. Remover acesso");
            Console.WriteLine("5. Reconstruir índice de acessos");
            Console.WriteLine("6. Voltar ao menu anterior");
            Console.Write("Escolha uma opção: ");

            string choice = Console.ReadLine();
            switch (choice)
            {
                case "1":
                    ConsultarAcessoPorUserID(acessosBinFilePath, acessoIndexFilePath);
                    break;
                case "2":
                    LerTodosAcessos(acessosBinFilePath);
                    break;
                case "3":
                    AddAcesso(acessosBinFilePath, acessoIndexFilePath);
                    break;
                case "4":
                    RemoveAcesso(acessosBinFilePath, acessoIndexFilePath);
                    break;
                case "5":
                    ReconstructAcessoDataAndIndex(acessosBinFilePath, acessoIndexFilePath);
                    Console.WriteLine("Índice reconstruído com sucesso. Pressione Enter para continuar.");
                    Console.ReadLine();
                    break;
                case "6":
                    return;
                default:
                    Console.WriteLine("Opção inválida. Pressione Enter para continuar.");
                    Console.ReadLine();
                    break;
            }
        }
    }

    public static void ConsultarAcessoPorUserID(string acessosBinFilePath, string acessoIndexFilePath)
    {
        Console.Write("Digite o UserID para consulta: ");
        string input = Console.ReadLine();
        if (long.TryParse(input, out long userId))
        {
            List<Acesso> acessos = BuscarAcessosPorUserID(acessosBinFilePath, acessoIndexFilePath, userId);
            if (acessos.Count > 0)
            {
                foreach (var a in acessos)
                {
                    Console.WriteLine($"UserID: {a.UserID}, EventType: {a.EventType}, EventTime: {a.EventTime}");
                }
            }
            else
            {
                Console.WriteLine("Nenhum acesso encontrado para o UserID informado.");
            }
        }
        else
        {
            Console.WriteLine("UserID inválido.");
        }
        Console.WriteLine("Pressione Enter para voltar ao menu anterior.");
        Console.ReadLine();
    }

    public static List<Acesso> BuscarAcessosPorUserID(string acessosBinFilePath, string acessoIndexFilePath, long userId)
    {
        List<Acesso> acessosEncontrados = new List<Acesso>();

        List<AcessoIndexEntry> indexEntries = LoadAcessoIndex(acessoIndexFilePath);

        // Encontrar as entradas com o UserID
        int index = indexEntries.BinarySearch(new AcessoIndexEntry { UserID = userId }, Comparer<AcessoIndexEntry>.Create((a, b) => a.UserID.CompareTo(b.UserID)));

        if (index < 0)
            index = ~index;

        // Deixa na posição correta no arquivo de index
        int startIndex = index;
        while (startIndex > 0 && indexEntries[startIndex - 1].UserID == userId)
        {
            startIndex--;
        }

        // Percorrer para frente para coletar todos os acessos
        for (int i = startIndex; i < indexEntries.Count && indexEntries[i].UserID == userId; i++)
        {
            long offset = indexEntries[i].Offset;

            using (var fs = new FileStream(acessosBinFilePath, FileMode.Open, FileAccess.Read))
            {
                fs.Seek(offset, SeekOrigin.Begin);
                byte[] record = new byte[Acesso.TamanhoRegistro];
                fs.Read(record, 0, Acesso.TamanhoRegistro);
                Acesso a = Acesso.FromBytes(record);
                if (!a.Deleted)
                    acessosEncontrados.Add(a);
            }
        }

        return acessosEncontrados;
    }

    public static void LerTodosAcessos(string acessosBinFilePath)
    {
        using (var fs = new FileStream(acessosBinFilePath, FileMode.Open, FileAccess.Read))
        using (var br = new BinaryReader(fs))
        {
            while (fs.Position < fs.Length)
            {
                byte[] record = br.ReadBytes(Acesso.TamanhoRegistro);
                if (record.Length < Acesso.TamanhoRegistro)
                {
                    Console.WriteLine("Registro incompleto encontrado. Ignorando.");
                    break;
                }

                Acesso a = Acesso.FromBytes(record);
                if (!a.Deleted)
                {
                    Console.WriteLine($"UserID: {a.UserID}, EventType: {a.EventType}, EventTime: {a.EventTime}");
                }
            }
        }
        Console.WriteLine("Pressione Enter para voltar ao menu anterior.");
        Console.ReadLine();
    }
}

public interface IByteSerializable
{
    byte[] ToBytes();
}

public class ProdutoIndexEntry
{
    public long ProductID;
    public long Offset;
}

public class AcessoIndexEntry
{
    public long UserID;
    public long Offset;
}

public class Produto : IByteSerializable
{
    public const int TamanhoRegistro = 117; // 8 + 8 + 100 + 1 bytes

    public long ProductID; // 8 bytes
    public double Preco;  // 8 bytes
    public string Categoria; // 100 bytes
    public bool Deleted; // 1 byte

    public byte[] ToBytes()
    {
        using (var ms = new MemoryStream(TamanhoRegistro))
        using (var bw = new BinaryWriter(ms))
        {
            bw.Write(ProductID);
            bw.Write(Preco);

            // Ajustar a string para ter tamanho fixo
            string categoriaAjustada = (Categoria ?? string.Empty).PadRight(100).Substring(0, 100);
            bw.Write(Encoding.ASCII.GetBytes(categoriaAjustada));

            bw.Write(Deleted ? (byte)1 : (byte)0); // Escrever o campo Deleted

            return ms.ToArray();
        }
    }

    public static Produto FromBytes(byte[] data)
    {
        if (data.Length != TamanhoRegistro)
            throw new ArgumentException($"Dados inválidos. Esperado {TamanhoRegistro} bytes, e recebeu {data.Length} bytes.");

        using (var ms = new MemoryStream(data))
        using (var br = new BinaryReader(ms))
        {
            Produto p = new Produto();
            p.ProductID = br.ReadInt64();
            p.Preco = br.ReadDouble();

            byte[] categoriaBytes = br.ReadBytes(100);
            p.Categoria = Encoding.ASCII.GetString(categoriaBytes).TrimEnd();

            p.Deleted = br.ReadByte() == 1; // Le o campo Deleted

            return p;
        }
    }
}

public class Acesso : IByteSerializable
{
    public const int TamanhoRegistro = 37; // 8 + 20 + 8 + 1

    public long UserID; // 8 bytes
    public string EventType; // 20 bytes
    public DateTime EventTime; // 8 bytes
    public bool Deleted; // 1 byte

    public byte[] ToBytes()
    {
        using (var ms = new MemoryStream(TamanhoRegistro))
        using (var bw = new BinaryWriter(ms))
        {
            bw.Write(UserID);

            string eventTypeAjustado = (EventType ?? string.Empty).PadRight(20).Substring(0, 20);
            bw.Write(Encoding.ASCII.GetBytes(eventTypeAjustado));

            bw.Write(EventTime.ToBinary());

            bw.Write(Deleted ? (byte)1 : (byte)0);

            return ms.ToArray();
        }
    }

    public static Acesso FromBytes(byte[] data)
    {
        if (data.Length != TamanhoRegistro)
            throw new ArgumentException($"Dados inválidos. Esperado {TamanhoRegistro} bytes, e recebeu {data.Length} bytes.");

        using (var ms = new MemoryStream(data))
        using (var br = new BinaryReader(ms))
        {
            Acesso a = new Acesso();
            a.UserID = br.ReadInt64();

            byte[] eventTypeBytes = br.ReadBytes(20);
            a.EventType = Encoding.ASCII.GetString(eventTypeBytes).TrimEnd();

            long eventTimeBinary = br.ReadInt64();
            a.EventTime = DateTime.FromBinary(eventTimeBinary);

            a.Deleted = br.ReadByte() == 1;

            return a;
        }
    }
}
