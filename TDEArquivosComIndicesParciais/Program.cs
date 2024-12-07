using System;
using System.IO;
using System.Globalization;
using System.Text;
using System.Collections.Generic;
using System.Diagnostics;
using System.Collections;

public class Program
{
    public static BPlusTree bPlusTreeProdutos;
    public static HashTable hashTableCategorias;

    // Dicionário para mapear ProductID -> (Categoria, Offset) em memória
    public static Dictionary<long, (string Categoria, long Offset)> memoryCatalog = new Dictionary<long, (string, long)>();

    public static void Main(string[] args)
    {
        string path = @"C:\\Users\\joaov\\source\\repos\\TDEArquivosComIndicesParciais\\TDEArquivosComIndicesParciais\\2019-Nov.csv";
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
        // Medição do tempo de criação dos arquivos binários
        var swTotal = Stopwatch.StartNew();
        CreateBinaryFiles(csvFilePath, produtosBinFilePath, acessosBinFilePath);

        // Medição do tempo para reconstrução do índice de produtos
        var swProdIndex = Stopwatch.StartNew();
        ReconstructProdutoDataAndIndex(produtosBinFilePath, produtoIndexFilePath);
        swProdIndex.Stop();
        Console.WriteLine($"Tempo para criar índice de produtos (arquivo): {swProdIndex.Elapsed}");

        // Medição do tempo para reconstrução do índice de acessos
        var swAcessoIndex = Stopwatch.StartNew();
        ReconstructAcessoDataAndIndex(acessosBinFilePath, acessoIndexFilePath);
        swAcessoIndex.Stop();
        Console.WriteLine($"Tempo para criar índice de acessos (arquivo): {swAcessoIndex.Elapsed}");

        swTotal.Stop();
        Console.WriteLine($"Tempo total para criação dos arquivos binários e índices: {swTotal.Elapsed}");

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

    public static void BPlusReconstructProdutoDataAndIndex(string produtosBinFilePath)
    {
        int order = 4;
        bPlusTreeProdutos = new BPlusTree(order);
        int hashTableSize = 1009;
        hashTableCategorias = new HashTable(hashTableSize);

        memoryCatalog.Clear(); // Limpa o catálogo de memória para reconstruir

        var stopwatch = Stopwatch.StartNew();
        using (var inputFs = new FileStream(produtosBinFilePath, FileMode.Open, FileAccess.Read))
        {
            long offset = 0;
            byte[] record = new byte[Produto.TamanhoRegistro];

            while (inputFs.Position < inputFs.Length)
            {
                int bytesRead = inputFs.Read(record, 0, Produto.TamanhoRegistro);
                if (bytesRead < Produto.TamanhoRegistro)
                {
                    Console.WriteLine("Registro incompleto encontrado. Ignorando.");
                    break;
                }

                Produto p = Produto.FromBytes(record);

                if (!p.Deleted)
                {
                    bPlusTreeProdutos.Insert(p.ProductID, offset);
                    hashTableCategorias.Insert(p.Categoria, offset);

                    memoryCatalog[p.ProductID] = (p.Categoria, offset);
                }

                offset += Produto.TamanhoRegistro;
            }
        }

        stopwatch.Stop();
        Console.WriteLine($"Tempo para criar os índices em memória (B+ e Hash): {stopwatch.Elapsed}");
    }

    public static Produto BPlusBuscarProdutoPorID(string produtosBinFilePath, long productId)
    {
        var stopwatch = Stopwatch.StartNew();
        long? offset = bPlusTreeProdutos.Search(productId);
        if (offset.HasValue)
        {
            using (var dataFs = new FileStream(produtosBinFilePath, FileMode.Open, FileAccess.Read))
            {
                dataFs.Seek(offset.Value, SeekOrigin.Begin);
                byte[] record = new byte[Produto.TamanhoRegistro];
                dataFs.Read(record, 0, Produto.TamanhoRegistro);
                Produto p = Produto.FromBytes(record);
                if (!p.Deleted)
                {
                    stopwatch.Stop();
                    Console.WriteLine($"Tempo para consultar pela B+ tree: {stopwatch.Elapsed}");
                    return p;
                }
            }
        }
        stopwatch.Stop();
        Console.WriteLine($"Tempo para consultar pela B+ tree (não encontrado): {stopwatch.Elapsed}");
        return null;
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
        var sw = Stopwatch.StartNew();

        string tempProdutosBinFilePath = produtosBinFilePath + ".tmp";
        string tempProdutoIndexFilePath = produtoIndexFilePath + ".tmp";

        using (var inputFs = new FileStream(produtosBinFilePath, FileMode.Open, FileAccess.Read))
        using (var outputFs = new FileStream(tempProdutosBinFilePath, FileMode.Create, FileAccess.Write))
        using (var indexFs = new FileStream(tempProdutoIndexFilePath, FileMode.Create, FileAccess.Write))
        using (var indexBw = new BinaryWriter(indexFs))
        {
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
                    outputFs.Write(record, 0, Produto.TamanhoRegistro);
                    indexBw.Write(p.ProductID);
                    indexBw.Write(newOffset);
                    newOffset += Produto.TamanhoRegistro;
                }
            }
        }

        File.Delete(produtosBinFilePath);
        File.Move(tempProdutosBinFilePath, produtosBinFilePath);

        File.Delete(produtoIndexFilePath);
        File.Move(tempProdutoIndexFilePath, produtoIndexFilePath);

        sw.Stop();
        Console.WriteLine($"Reconstrução completa (produtos). Tempo: {sw.Elapsed}");
    }

    public static void ReadProdutosBin(string produtosBinFilePath, string produtoIndexFilePath)
    {
        while (true)
        {
            Console.Clear();
            Console.WriteLine("Menu do Arquivo 'produtos.bin':");
            Console.WriteLine("1. Consultar por ProductID (via índice)");
            Console.WriteLine("2. Consultar por ProductID (via B+ tree)");
            Console.WriteLine("3. Consultar por Categoria (via tabela hash)");
            Console.WriteLine("4. Ler todo o arquivo");
            Console.WriteLine("5. Adicionar produto");
            Console.WriteLine("6. Remover produto");
            Console.WriteLine("7. Reconstruir índice de produtos");
            Console.WriteLine("8. Reconstruir BTree+ e tabela hash de produtos");
            Console.WriteLine("10. Adicionar produto apenas nos índices em memória (B+)");
            Console.WriteLine("11. Remover produto apenas nos índices em memória (B+)");
            Console.WriteLine("12. Adicionar produto apenas nos índices em memória (Hash)");
            Console.WriteLine("13. Remover produto apenas nos índices em memória (Hash)");
            Console.WriteLine("14. Voltar ao menu anterior");
            Console.Write("Escolha uma opção: ");

            string choice = Console.ReadLine();
            switch (choice)
            {
                case "1":
                    ConsultarProdutoPorIDViaIndice(produtosBinFilePath, produtoIndexFilePath);
                    break;
                case "2":
                    ConsultarProdutoPorIDViaBPlusTree(produtosBinFilePath);
                    break;
                case "3":
                    HashConsultarProdutosPorCategoria(produtosBinFilePath);
                    break;
                case "4":
                    LerTodosProdutos(produtosBinFilePath);
                    break;
                case "5":
                    AddProduto(produtosBinFilePath, produtoIndexFilePath);
                    break;
                case "6":
                    RemoveProduto(produtosBinFilePath, produtoIndexFilePath);
                    break;
                case "7":
                    {
                        var sw = Stopwatch.StartNew();
                        ReconstructProdutoDataAndIndex(produtosBinFilePath, produtoIndexFilePath);
                        sw.Stop();
                        Console.WriteLine("Índice reconstruído com sucesso.");
                        Console.WriteLine($"Tempo: {sw.Elapsed}. Pressione Enter para continuar.");
                        Console.ReadLine();
                        break;
                    }
                case "8":
                    {
                        var sw = Stopwatch.StartNew();
                        BPlusReconstructProdutoDataAndIndex(produtosBinFilePath);
                        sw.Stop();
                        Console.WriteLine("BTree+ e tabela hash reconstruídos com sucesso.");
                        Console.WriteLine($"Tempo: {sw.Elapsed}. Pressione Enter para continuar.");
                        Console.ReadLine();
                        break;
                    }
                case "10":
                    AddProdutoEmMemoriaB();
                    break;
                case "11":
                    RemoveProdutoEmMemoriaB();
                    break;
                case "12":
                    AddProdutoEmMemoriaHash();
                    break;
                case "13":
                    RemoveProdutoEmMemoriaHash();
                    break;
                case "14":
                    return;
                default:
                    Console.WriteLine("Opção inválida. Pressione Enter para continuar.");
                    Console.ReadLine();
                    break;
            }
        }
    }

    public static void AddProdutoEmMemoriaB()
    {
        if (bPlusTreeProdutos == null)
        {
            Console.WriteLine("Índices em memória (B+) não criados. Reconstruindo...");
            string produtosBinFilePath = @"..\..\produtos.bin";
            BPlusReconstructProdutoDataAndIndex(produtosBinFilePath);
        }

        Console.Write("Digite o ProductID: ");
        long productId = long.Parse(Console.ReadLine());
        Console.Write("Digite o Preço: ");
        double preco = double.Parse(Console.ReadLine());
        Console.Write("Digite a Categoria: ");
        string categoria = Console.ReadLine();

        var sw = Stopwatch.StartNew();
        long simulatedOffset = -1;
        bPlusTreeProdutos.Insert(productId, simulatedOffset);

        memoryCatalog[productId] = (categoria, simulatedOffset);
        sw.Stop();

        Console.WriteLine("Produto adicionado somente na B+ em memória.");
        Console.WriteLine($"Tempo da inclusão em memória (B+): {sw.Elapsed}");
        Console.WriteLine("Pressione Enter para continuar.");
        Console.ReadLine();
    }

    public static void RemoveProdutoEmMemoriaB()
    {
        if (bPlusTreeProdutos == null)
        {
            Console.WriteLine("Índices em memória (B+) não criados. Reconstruindo...");
            string produtosBinFilePath = @"..\..\produtos.bin";
            BPlusReconstructProdutoDataAndIndex(produtosBinFilePath);
        }

        Console.Write("Digite o ProductID para remover: ");
        long productId = long.Parse(Console.ReadLine());

        var sw = Stopwatch.StartNew();
        long? offset = bPlusTreeProdutos.Search(productId);
        if (offset.HasValue && memoryCatalog.ContainsKey(productId))
        {
            bPlusTreeProdutos.Remove(productId);
            memoryCatalog.Remove(productId);

            sw.Stop();
            Console.WriteLine("Produto removido somente da B+ em memória.");
            Console.WriteLine($"Tempo da remoção em memória (B+): {sw.Elapsed}");
        }
        else
        {
            sw.Stop();
            Console.WriteLine("Produto não encontrado na B+ em memória.");
        }

        Console.WriteLine("Pressione Enter para continuar.");
        Console.ReadLine();
    }

    public static void AddProdutoEmMemoriaHash()
    {
        if (hashTableCategorias == null)
        {
            Console.WriteLine("Índices em memória (Hash) não criados. Reconstruindo...");
            string produtosBinFilePath = @"..\..\produtos.bin";
            BPlusReconstructProdutoDataAndIndex(produtosBinFilePath);
        }

        Console.Write("Digite o ProductID: ");
        long productId = long.Parse(Console.ReadLine());
        Console.Write("Digite o Preço: ");
        double preco = double.Parse(Console.ReadLine());
        Console.Write("Digite a Categoria: ");
        string categoria = Console.ReadLine();

        var sw = Stopwatch.StartNew();
        long simulatedOffset = -1;
        hashTableCategorias.Insert(categoria, simulatedOffset);
        memoryCatalog[productId] = (categoria, simulatedOffset);
        sw.Stop();

        Console.WriteLine("Produto adicionado somente na Hash em memória.");
        Console.WriteLine($"Tempo da inclusão em memória (Hash): {sw.Elapsed}");
        Console.WriteLine("Pressione Enter para continuar.");
        Console.ReadLine();
    }

    public static void RemoveProdutoEmMemoriaHash()
    {
        if (hashTableCategorias == null)
        {
            Console.WriteLine("Índices em memória (Hash) não criados. Reconstruindo...");
            string produtosBinFilePath = @"..\..\produtos.bin";
            BPlusReconstructProdutoDataAndIndex(produtosBinFilePath);
        }

        Console.Write("Digite o ProductID para remover da Hash: ");
        long productId = long.Parse(Console.ReadLine());

        var sw = Stopwatch.StartNew();
        if (memoryCatalog.ContainsKey(productId))
        {
            var (cat, off) = memoryCatalog[productId];
            hashTableCategorias.Remove(cat, off);

            memoryCatalog.Remove(productId);

            sw.Stop();
            Console.WriteLine("Produto removido somente da Hash em memória.");
            Console.WriteLine($"Tempo da remoção em memória (Hash): {sw.Elapsed}");
        }
        else
        {
            sw.Stop();
            Console.WriteLine("Produto não encontrado na Hash em memória.");
        }

        Console.WriteLine("Pressione Enter para continuar.");
        Console.ReadLine();
    }


    public static void ConsultarProdutoPorIDViaIndice(string produtosBinFilePath, string produtoIndexFilePath)
    {
        Console.Write("Digite o ProductID para consulta: ");
        string input = Console.ReadLine();
        if (long.TryParse(input, out long productId))
        {
            var sw = Stopwatch.StartNew();
            Produto produto = BuscarProdutoPorID(produtosBinFilePath, produtoIndexFilePath, productId);
            sw.Stop();
            if (produto != null)
            {
                Console.WriteLine($"ProductID: {produto.ProductID}, Preço: {produto.Preco}, Categoria: {produto.Categoria}");
            }
            else
            {
                Console.WriteLine("Produto não encontrado.");
            }
            Console.WriteLine($"Tempo da consulta via índice de arquivo: {sw.Elapsed}");
        }
        else
        {
            Console.WriteLine("ProductID inválido.");
        }
        Console.WriteLine("Pressione Enter para voltar ao menu anterior.");
        Console.ReadLine();
    }

    public static void ConsultarProdutoPorIDViaBPlusTree(string produtosBinFilePath)
    {
        if (bPlusTreeProdutos == null)
        {
            Console.WriteLine("A B+ tree não foi construída. Reconstruindo...");
            BPlusReconstructProdutoDataAndIndex(produtosBinFilePath);
        }
        Console.Write("Digite o ProductID para consulta: ");
        string input = Console.ReadLine();
        if (long.TryParse(input, out long productId))
        {
            Produto produto = BPlusBuscarProdutoPorID(produtosBinFilePath, productId);
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


    public static void HashConsultarProdutosPorCategoria(string produtosBinFilePath)
    {
        if (hashTableCategorias == null)
        {
            Console.WriteLine("A tabela hash não foi construída. Reconstruindo...");
            BPlusReconstructProdutoDataAndIndex(produtosBinFilePath);
        }

        Console.Write("Digite a Categoria para consulta: ");
        string categoria = Console.ReadLine();
        var sw = Stopwatch.StartNew();
        List<long> offsets = hashTableCategorias.Search(categoria);
        sw.Stop();

        if (offsets.Count > 0)
        {
            using (var dataFs = new FileStream(produtosBinFilePath, FileMode.Open, FileAccess.Read))
            {
                // Limita a iteração para as primeiras 5 ocorrências
                foreach (long offset in offsets.Take(5))
                {
                    dataFs.Seek(offset, SeekOrigin.Begin);
                    byte[] record = new byte[Produto.TamanhoRegistro];
                    dataFs.Read(record, 0, Produto.TamanhoRegistro);
                    Produto p = Produto.FromBytes(record);
                    if (!p.Deleted)
                    {
                        Console.WriteLine($"ProductID: {p.ProductID}, Preço: {p.Preco}, Categoria: {p.Categoria}");
                    }
                }

                // Verifica se há mais de 5 ocorrências 
                if (offsets.Count > 5)
                {
                    Console.WriteLine($"Foram exibidas as primeiras 5 ocorrências de '{categoria}'.");
                }
            }
        }
        else
        {
            Console.WriteLine("Nenhum produto encontrado para a categoria informada.");
        }
        Console.WriteLine($"Tempo da consulta via hash em memória: {sw.Elapsed}");
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
        var sw = Stopwatch.StartNew();
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
        sw.Stop();
        Console.WriteLine($"Tempo para ler todo o arquivo: {sw.Elapsed}");
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

        var sw = Stopwatch.StartNew();

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
                    outputBw.Write(productId);
                    outputBw.Write(offset);
                    inserted = true;
                }

                if (currentProductId == productId)
                {
                    Console.WriteLine("ProductID já existe. Operação abortada.");
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
                outputBw.Write(productId);
                outputBw.Write(offset);
            }
        }

        File.Delete(produtoIndexFilePath);
        File.Move(tempIndexFilePath, produtoIndexFilePath);

        if (bPlusTreeProdutos != null)
            bPlusTreeProdutos.Insert(p.ProductID, offset);

        if (hashTableCategorias != null)
            hashTableCategorias.Insert(p.Categoria, offset);

        sw.Stop();
        Console.WriteLine("Produto adicionado com sucesso!");
        Console.WriteLine($"Tempo da inclusão: {sw.Elapsed}");
        Console.WriteLine("Pressione Enter para continuar.");
        Console.ReadLine();
    }

    public static void RemoveProduto(string produtosBinFilePath, string produtoIndexFilePath)
    {
        Console.Write("Digite o ProductID para remover: ");
        long productId = long.Parse(Console.ReadLine());
        var sw = Stopwatch.StartNew();

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
            using (var fs = new FileStream(produtosBinFilePath, FileMode.Open, FileAccess.Write))
            {
                fs.Seek(offsetToDelete + Produto.TamanhoRegistro - 1, SeekOrigin.Begin);
                fs.WriteByte(1);
            }

            if (bPlusTreeProdutos != null)
                bPlusTreeProdutos.Remove(productId);

            using (var fs = new FileStream(produtosBinFilePath, FileMode.Open, FileAccess.Read))
            {
                fs.Seek(offsetToDelete, SeekOrigin.Begin);
                byte[] record = new byte[Produto.TamanhoRegistro];
                fs.Read(record, 0, Produto.TamanhoRegistro);
                Produto p = Produto.FromBytes(record);
                if (hashTableCategorias != null)
                    hashTableCategorias.Remove(p.Categoria, offsetToDelete);
            }

            File.Delete(produtoIndexFilePath);
            File.Move(tempIndexFilePath, produtoIndexFilePath);

            sw.Stop();
            Console.WriteLine("Produto removido com sucesso!");
            Console.WriteLine($"Tempo da remoção: {sw.Elapsed}");
        }
        else
        {
            File.Delete(tempIndexFilePath);
            Console.WriteLine("Produto não encontrado.");
        }

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

        var sw = Stopwatch.StartNew();
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
                    outputBw.Write(userId);
                    outputBw.Write(offset);
                    inserted = true;
                }

                outputBw.Write(currentUserId);
                outputBw.Write(currentOffset);
            }

            if (!inserted)
            {
                outputBw.Write(userId);
                outputBw.Write(offset);
            }
        }

        File.Delete(acessoIndexFilePath);
        File.Move(tempIndexFilePath, acessoIndexFilePath);
        sw.Stop();
        Console.WriteLine("Acesso adicionado com sucesso!");
        Console.WriteLine($"Tempo da inclusão: {sw.Elapsed}");
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

        var sw = Stopwatch.StartNew();
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
                            found = true;
                            offsetToDelete = currentOffset;
                            continue;
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
                fs.WriteByte(1); // marca deletado
            }

            File.Delete(acessoIndexFilePath);
            File.Move(tempIndexFilePath, acessoIndexFilePath);

            sw.Stop();
            Console.WriteLine("Acesso removido com sucesso!");
            Console.WriteLine($"Tempo da remoção: {sw.Elapsed}");
        }
        else
        {
            File.Delete(tempIndexFilePath);
            Console.WriteLine("Acesso não encontrado.");
        }

        Console.WriteLine("Pressione Enter para continuar.");
        Console.ReadLine();
    }

    public static void ReconstructAcessoDataAndIndex(string acessosBinFilePath, string acessoIndexFilePath)
    {
        var sw = Stopwatch.StartNew();
        string tempAcessosBinFilePath = acessosBinFilePath + ".tmp";
        string tempAcessoIndexFilePath = acessoIndexFilePath + ".tmp";

        using (var inputFs = new FileStream(acessosBinFilePath, FileMode.Open, FileAccess.Read))
        using (var outputFs = new FileStream(tempAcessosBinFilePath, FileMode.Create, FileAccess.Write))
        using (var indexFs = new FileStream(tempAcessoIndexFilePath, FileMode.Create, FileAccess.Write))
        using (var indexBw = new BinaryWriter(indexFs))
        {
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
                    outputFs.Write(record, 0, Acesso.TamanhoRegistro);
                    indexBw.Write(a.UserID);
                    indexBw.Write(newOffset);
                    newOffset += Acesso.TamanhoRegistro;
                }
            }
        }

        File.Delete(acessosBinFilePath);
        File.Move(tempAcessosBinFilePath, acessosBinFilePath);

        File.Delete(acessoIndexFilePath);
        File.Move(tempAcessoIndexFilePath, acessoIndexFilePath);

        sw.Stop();
        Console.WriteLine("Reconstrução completa (acessos).");
        Console.WriteLine($"Tempo: {sw.Elapsed}");
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
                    {
                        var sw = Stopwatch.StartNew();
                        ReconstructAcessoDataAndIndex(acessosBinFilePath, acessoIndexFilePath);
                        sw.Stop();
                        Console.WriteLine($"Índice reconstruído com sucesso. Tempo: {sw.Elapsed}. Pressione Enter para continuar.");
                        Console.ReadLine();
                        break;
                    }
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
            var sw = Stopwatch.StartNew();
            List<Acesso> acessos = BuscarAcessosPorUserID(acessosBinFilePath, acessoIndexFilePath, userId);
            sw.Stop();
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
            Console.WriteLine($"Tempo da consulta via índice de arquivo: {sw.Elapsed}");
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

        int index = indexEntries.BinarySearch(new AcessoIndexEntry { UserID = userId }, Comparer<AcessoIndexEntry>.Create((a, b) => a.UserID.CompareTo(b.UserID)));

        if (index < 0)
            index = ~index;

        int startIndex = index;
        while (startIndex > 0 && startIndex < indexEntries.Count && indexEntries[startIndex - 1].UserID == userId)
        {
            startIndex--;
        }

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
        var sw = Stopwatch.StartNew();
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
        sw.Stop();
        Console.WriteLine($"Tempo para ler todo o arquivo de acessos: {sw.Elapsed}");
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

            string categoriaAjustada = (Categoria ?? string.Empty).PadRight(100).Substring(0, 100);
            bw.Write(Encoding.ASCII.GetBytes(categoriaAjustada));

            bw.Write(Deleted ? (byte)1 : (byte)0);
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

            p.Deleted = br.ReadByte() == 1;
            return p;
        }
    }
}

public class BPlusTreeNode
{
    public bool IsLeaf { get; set; }
    public List<long> Keys { get; set; }
    public List<long> Offsets { get; set; }
    public List<BPlusTreeNode> Children { get; set; }
    public BPlusTreeNode Next { get; set; }

    public BPlusTreeNode(bool isLeaf)
    {
        IsLeaf = isLeaf;
        Keys = new List<long>();
        if (IsLeaf)
        {
            Offsets = new List<long>();
            Children = null;
            Next = null;
        }
        else
        {
            Offsets = null;
            Children = new List<BPlusTreeNode>();
            Next = null;
        }
    }
}

public class BPlusTree
{
    private int Order;
    private BPlusTreeNode Root;

    public BPlusTree(int order)
    {
        Order = order;
        Root = new BPlusTreeNode(true);
    }

    public void Insert(long key, long offset)
    {
        BPlusTreeNode root = Root;
        if (root.Keys.Count == 2 * Order - 1)
        {
            BPlusTreeNode newRoot = new BPlusTreeNode(false);
            newRoot.Children.Add(root);
            SplitChild(newRoot, 0);
            Root = newRoot;
            InsertNonFull(newRoot, key, offset);
        }
        else
        {
            InsertNonFull(root, key, offset);
        }
    }

    private void InsertNonFull(BPlusTreeNode node, long key, long offset)
    {
        int i = node.Keys.Count - 1;
        if (node.IsLeaf)
        {
            while (i >= 0 && key < node.Keys[i])
            {
                i--;
            }
            node.Keys.Insert(i + 1, key);
            node.Offsets.Insert(i + 1, offset);
        }
        else
        {
            while (i >= 0 && key < node.Keys[i])
            {
                i--;
            }
            i++;
            if (node.Children[i].Keys.Count == 2 * Order - 1)
            {
                SplitChild(node, i);
                if (key > node.Keys[i])
                {
                    i++;
                }
            }
            InsertNonFull(node.Children[i], key, offset);
        }
    }

    private void SplitChild(BPlusTreeNode parent, int index)
    {
        BPlusTreeNode nodeToSplit = parent.Children[index];
        BPlusTreeNode newNode = new BPlusTreeNode(nodeToSplit.IsLeaf);

        int t = Order;

        newNode.Keys.AddRange(nodeToSplit.Keys.GetRange(t, t - 1));
        nodeToSplit.Keys.RemoveRange(t, t - 1);

        if (nodeToSplit.IsLeaf)
        {
            newNode.Offsets.AddRange(nodeToSplit.Offsets.GetRange(t, t - 1));
            nodeToSplit.Offsets.RemoveRange(t, t - 1);

            newNode.Next = nodeToSplit.Next;
            nodeToSplit.Next = newNode;
        }
        else
        {
            newNode.Children.AddRange(nodeToSplit.Children.GetRange(t, t));
            nodeToSplit.Children.RemoveRange(t, t);
        }

        parent.Keys.Insert(index, nodeToSplit.Keys[t - 1]);
        nodeToSplit.Keys.RemoveAt(t - 1);
        parent.Children.Insert(index + 1, newNode);
    }

    public long? Search(long key)
    {
        return Search(Root, key);
    }

    private long? Search(BPlusTreeNode node, long key)
    {
        int i = 0;
        while (i < node.Keys.Count && key > node.Keys[i])
        {
            i++;
        }

        if (node.IsLeaf)
        {
            if (i < node.Keys.Count && node.Keys[i] == key)
            {
                return node.Offsets[i];
            }
            else
            {
                return null;
            }
        }
        else
        {
            return Search(node.Children[i], key);
        }
    }

    public void Remove(long key)
    {
        Remove(Root, key);
        if (Root.Keys.Count == 0 && !Root.IsLeaf)
        {
            Root = Root.Children[0];
        }
    }

    private void Remove(BPlusTreeNode node, long key)
    {
        int idx = node.Keys.FindIndex(k => k == key);
        if (node.IsLeaf)
        {
            if (idx != -1)
            {
                node.Keys.RemoveAt(idx);
                node.Offsets.RemoveAt(idx);

                if (node != Root && node.Keys.Count < Order - 1)
                {
                    Rebalance(node);
                }
            }
            else
            {
                Console.WriteLine("Chave não encontrada.");
            }
        }
        else
        {
            int childIdx = 0;
            while (childIdx < node.Keys.Count && key > node.Keys[childIdx])
            {
                childIdx++;
            }

            Remove(node.Children[childIdx], key);

            if (node.Children[childIdx].Keys.Count < Order - 1)
            {
                RebalanceChild(node, childIdx);
            }
        }
    }

    private void Rebalance(BPlusTreeNode node)
    {
        BPlusTreeNode parent = FindParent(Root, node);
        if (parent == null) return;

        int idx = parent.Children.IndexOf(node);

        BPlusTreeNode leftSibling = idx > 0 ? parent.Children[idx - 1] : null;
        BPlusTreeNode rightSibling = idx < parent.Children.Count - 1 ? parent.Children[idx + 1] : null;

        if (leftSibling != null && leftSibling.Keys.Count > Order - 1)
        {
            node.Keys.Insert(0, leftSibling.Keys.Last());
            node.Offsets.Insert(0, leftSibling.Offsets.Last());
            leftSibling.Keys.RemoveAt(leftSibling.Keys.Count - 1);
            leftSibling.Offsets.RemoveAt(leftSibling.Offsets.Count - 1);

            parent.Keys[idx - 1] = node.Keys.First();
        }
        else if (rightSibling != null && rightSibling.Keys.Count > Order - 1)
        {
            node.Keys.Add(rightSibling.Keys.First());
            node.Offsets.Add(rightSibling.Offsets.First());
            rightSibling.Keys.RemoveAt(0);
            rightSibling.Offsets.RemoveAt(0);

            parent.Keys[idx] = rightSibling.Keys.First();
        }
        else
        {
            if (leftSibling != null)
            {
                leftSibling.Keys.AddRange(node.Keys);
                leftSibling.Offsets.AddRange(node.Offsets);
                leftSibling.Next = node.Next;

                parent.Keys.RemoveAt(idx - 1);
                parent.Children.RemoveAt(idx);
            }
            else if (rightSibling != null)
            {
                node.Keys.AddRange(rightSibling.Keys);
                node.Offsets.AddRange(rightSibling.Offsets);
                node.Next = rightSibling.Next;

                parent.Keys.RemoveAt(idx);
                parent.Children.RemoveAt(idx + 1);
            }

            if (parent == Root && parent.Keys.Count == 0)
            {
                Root = node;
            }
        }
    }

    private void RebalanceChild(BPlusTreeNode parent, int idx)
    {
        BPlusTreeNode node = parent.Children[idx];
        BPlusTreeNode leftSibling = idx > 0 ? parent.Children[idx - 1] : null;
        BPlusTreeNode rightSibling = idx < parent.Children.Count - 1 ? parent.Children[idx + 1] : null;

        if (leftSibling != null && leftSibling.Keys.Count > Order - 1)
        {
            node.Keys.Insert(0, parent.Keys[idx - 1]);
            parent.Keys[idx - 1] = leftSibling.Keys.Last();
            if (!node.IsLeaf)
            {
                node.Children.Insert(0, leftSibling.Children.Last());
                leftSibling.Children.RemoveAt(leftSibling.Children.Count - 1);
            }
            leftSibling.Keys.RemoveAt(leftSibling.Keys.Count - 1);
        }
        else if (rightSibling != null && rightSibling.Keys.Count > Order - 1)
        {
            node.Keys.Add(parent.Keys[idx]);
            parent.Keys[idx] = rightSibling.Keys.First();
            if (!node.IsLeaf)
            {
                node.Children.Add(rightSibling.Children.First());
                rightSibling.Children.RemoveAt(0);
            }
            rightSibling.Keys.RemoveAt(0);
        }
        else
        {
            if (leftSibling != null)
            {
                leftSibling.Keys.Add(parent.Keys[idx - 1]);
                leftSibling.Keys.AddRange(node.Keys);
                if (!node.IsLeaf)
                {
                    leftSibling.Children.AddRange(node.Children);
                }

                parent.Keys.RemoveAt(idx - 1);
                parent.Children.RemoveAt(idx);
            }
            else if (rightSibling != null)
            {
                node.Keys.Add(parent.Keys[idx]);
                node.Keys.AddRange(rightSibling.Keys);
                if (!node.IsLeaf)
                {
                    node.Children.AddRange(rightSibling.Children);
                }

                parent.Keys.RemoveAt(idx);
                parent.Children.RemoveAt(idx + 1);
            }

            if (parent == Root && parent.Keys.Count == 0)
            {
                Root = node;
            }
        }
    }

    private BPlusTreeNode FindParent(BPlusTreeNode current, BPlusTreeNode child)
    {
        if (current.IsLeaf || current.Children == null)
            return null;

        foreach (var c in current.Children)
        {
            if (c == child)
                return current;
            else
            {
                var result = FindParent(c, child);
                if (result != null)
                    return result;
            }
        }
        return null;
    }
}

public class HashEntry
{
    public string Key { get; set; }
    public List<long> Offsets { get; set; }

    public HashEntry(string key)
    {
        Key = key;
        Offsets = new List<long>();
    }
}

public class HashTable
{
    private int Size;
    private List<HashEntry>[] Table;

    public HashTable(int size)
    {
        Size = size;
        Table = new List<HashEntry>[Size];
        for (int i = 0; i < Size; i++)
        {
            Table[i] = new List<HashEntry>();
        }
    }

    public void Insert(string key, long offset)
    {
        int index = HashFunction(key);
        var bucket = Table[index];
        var entry = bucket.Find(e => e.Key == key);
        if (entry == null)
        {
            entry = new HashEntry(key);
            bucket.Add(entry);
        }
        entry.Offsets.Add(offset);
    }

    public List<long> Search(string key)
    {
        int index = HashFunction(key);
        var bucket = Table[index];
        var entry = bucket.Find(e => e.Key == key);
        if (entry != null)
        {
            return entry.Offsets;
        }
        return new List<long>();
    }

    public void Remove(string key, long offset)
    {
        int index = HashFunction(key);
        var bucket = Table[index];
        var entry = bucket.Find(e => e.Key == key);
        if (entry != null)
        {
            entry.Offsets.Remove(offset);
            if (entry.Offsets.Count == 0)
            {
                bucket.Remove(entry);
            }
        }
    }

    private int HashFunction(string key)
    {
        return Math.Abs(key.GetHashCode()) % Size;
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
