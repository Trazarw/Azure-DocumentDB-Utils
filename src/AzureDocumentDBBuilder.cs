using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Client.TransientFaultHandling;
using Microsoft.Azure.Documents.Client.TransientFaultHandling.Strategies;
using System;
using System.Linq;

public interface IAzureDocumentDBBuilder
{
    IReliableReadWriteDocumentClient GetIReliableReadWriteDocumentClient(string endpoint, string auth, ConnectionPolicy connPolicy = null, DocumentDbRetryStrategy retryStrategy = null);
    Database GetDatabase(IReliableReadWriteDocumentClient client, string databaseId, bool createIfNotExist);
}

public class AzureDocumentDBBuilder : IAzureDocumentDBBuilder
{
    IReliableReadWriteDocumentClient IAzureDocumentDBBuilder.GetIReliableReadWriteDocumentClient(
    string endpoint,
    string auth,
    ConnectionPolicy connPolicy,
    DocumentDbRetryStrategy retryStrategy)
    {
        if (string.IsNullOrWhiteSpace(endpoint))
            throw new ArgumentNullException("endpoint");

        if (string.IsNullOrWhiteSpace(auth))
            throw new ArgumentNullException("auth");

        if (connPolicy == null)
            connPolicy = new ConnectionPolicy() { ConnectionMode = ConnectionMode.Direct };

        if (retryStrategy == null)
            retryStrategy = new DocumentDbRetryStrategy(DocumentDbRetryStrategy.DefaultExponential) { FastFirstRetry = true };

        return new DocumentClient(new Uri(endpoint), auth, connPolicy).AsReliable(retryStrategy);
    }

    Database IAzureDocumentDBBuilder.GetDatabase(
    IReliableReadWriteDocumentClient client,
    string databaseId,
    bool createIfNotExist)
    {
        if (client == null)
            throw new ArgumentNullException("client");

        if (string.IsNullOrWhiteSpace(databaseId))
            throw new ArgumentNullException("databaseId");

        Database database = client.CreateDatabaseQuery().Where(u => u.Id.ToString() == databaseId)
                                                        .AsEnumerable()
                                                        .FirstOrDefault();
        if (database == null && createIfNotExist)
            database = client.CreateDatabaseAsync(new Database { Id = databaseId }).Result;
        else if (database == null && !createIfNotExist)
            throw new InvalidOperationException("The database does not exist and you are not creating it at runtime");

        return database;
    }
}