using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client.TransientFaultHandling;
using System;
using System.Linq;
using System.Collections.Generic;

public interface IAzureDocumentDBCollectionSelfLinkCache
{
    Dictionary<string, string> Cache { get; }
}

public class AzureDocumentDBCollectionSelfLinkCache : IAzureDocumentDBCollectionSelfLinkCache
{
    private Dictionary<string, string> _internalCache;

    Dictionary<string, string> IAzureDocumentDBCollectionSelfLinkCache.Cache
    {
        get
        {
            return _internalCache;
        }
    }

    public AzureDocumentDBCollectionSelfLinkCache(IReliableReadWriteDocumentClient client, Database database, List<Type> types)
    {
        if (client == null)
            throw new ArgumentNullException("client");

        if (database == null)
            throw new ArgumentNullException("databaseId");

        if (types == null || types.Count == 0)
            throw new ArgumentNullException("types");

        this.Init(client, database, types);
    }

    private void Init(IReliableReadWriteDocumentClient client, Database database, List<Type> types)
    {
        _internalCache = new Dictionary<string, string>();

        foreach (Type type in types)
        {
            if (type.BaseType != typeof(Resource))
                throw new InvalidOperationException("All registered types should come from the base class Microsoft.Azure.Documents.Resource");

            if (_internalCache[type.Name] != null)
                throw new InvalidOperationException("The type name: " + type.Name + "already exist in the collection");

            DocumentCollection collection = client.CreateDocumentCollectionQuery(database.SelfLink)
                                                   .Where(c => c.Id == type.Name)
                                                   .ToArray()
                                                   .FirstOrDefault();
            if (collection == null)
                throw new InvalidOperationException("There is not any existing Azure DocumentCollection that matches the type name: " + type.Name);

            _internalCache.Add(type.Name, collection.SelfLink);
        }
    }
}