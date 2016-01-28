using System;
using System.Net;
using Elasticsearch.Net;

namespace Foundatio.Elasticsearch.Repositories.Configuration {
    public class KeepAliveHttpConnection : HttpConnection {
        protected override void AlterServicePoint(ServicePoint requestServicePoint, RequestData requestData) {
            base.AlterServicePoint(requestServicePoint, requestData);

            // TODO: Check to see if we need to still do keep alive as elastic is setting this as well.
            requestServicePoint.SetTcpKeepAlive(true, 30 * 1000, 2000);
        }
    }
}
