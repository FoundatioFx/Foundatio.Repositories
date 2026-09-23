from pathlib import Path
import sys
root=Path(sys.argv[1]);p=root/'docker-compose.yml';s=p.read_text()
old='''      discovery.type: single-node
      xpack.security.enabled: "false"'''
new='''      node.name: foundatio-es1
      cluster.name: foundatio-development
      discovery.seed_hosts: elasticsearch,elasticsearch-replica
      cluster.initial_master_nodes: foundatio-es1,foundatio-es2
      xpack.security.enabled: "false"'''
assert s.count(old)==1;s=s.replace(old,new)
marker='\n  kibana:';assert s.count(marker)==1
replica='''
  # Compatibility cutover restores the configured replica policy before deleting its source.
  # A second test node makes that durability gate executable in ordinary CI and local development.
  elasticsearch-replica:
    image: docker.elastic.co/elasticsearch/elasticsearch:${ELASTICSEARCH_VERSION:-9.5.0}
    environment:
      node.name: foundatio-es2
      cluster.name: foundatio-development
      discovery.seed_hosts: elasticsearch,elasticsearch-replica
      cluster.initial_master_nodes: foundatio-es1,foundatio-es2
      xpack.security.enabled: "false"
      action.destructive_requires_name: false
      ES_JAVA_OPTS: -Xms512m -Xmx512m
    networks:
      - foundatio
    healthcheck:
      interval: 2s
      retries: 30
      test: curl -s 'http://localhost:9200/_cluster/health?wait_for_nodes=2&timeout=1s' | grep -q '"timed_out":false'
'''
s=s.replace(marker,replica+marker)
s=s.replace("      test: curl -s http://localhost:9200/_cluster/health | grep -vq '\"status\":\"red\"'", "      test: curl -s 'http://localhost:9200/_cluster/health?wait_for_nodes=2&timeout=1s' | grep -q '\"timed_out\":false'")
s=s.replace('    command: elasticsearch:9200','    command: elasticsearch:9200 elasticsearch-replica:9200')
s=s.replace('    depends_on:\n      - elasticsearch\n','    depends_on:\n      - elasticsearch\n      - elasticsearch-replica\n')
p.write_text(s)
p=root/'docs/guide/index-management.md'
with p.open('a') as f:
 f.write('\nThe repository\'s local/test Docker Compose topology now provisions two Elasticsearch nodes so ordinary CI can exercise replica-ready cutover rather than hanging against a single node with an unassigned default replica. This adds one 512 MiB JVM heap plus native memory overhead. Application-facing ports and Kibana routing remain unchanged; the replica node is internal to the Compose network. This is test/development infrastructure, not a production-cluster configuration or a publishing workflow change. Custom single-node test fixtures must explicitly request zero replicas or expect the replica-readiness gate to refuse cutover.\n')
