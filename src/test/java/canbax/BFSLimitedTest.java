package canbax;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.driver.v1.StatementResult;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BFSLimitedTest {
    private static final Config driverConfig = Config.build().withoutEncryption().toConfig();
    private ServerControls embeddedDatabaseServer;


    @BeforeEach
    void initializeNeo4j() {
        this.embeddedDatabaseServer = TestServerBuilders.newInProcessBuilder().withProcedure(BFSLimited.class)
                .newServer();
    }

    @Test
    public void test1() {

        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
             Session session = driver.session()) {
            // And given I have a node in the database
            session.run("");

            // Then I can search for that node with lucene query syntax
            StatementResult result = session
                    .run("CALL graphOfInterest([5,7], [], 1, false, 100, 1, null, false, null, 2) YIELD nodes, edges return nodes, edges");

            Record r = result.single();
            Set<Long> nodeSet = r.get("nodes").asList().stream().map(x -> ((InternalNode) x).id())
                    .collect(Collectors.toSet());
            Set<Long> edgeSet = r.get("edges").asList().stream().map(x -> ((InternalRelationship) x).id())
                    .collect(Collectors.toSet());

            ArrayList<Long> trueNodeSet = new ArrayList<>();
            trueNodeSet.add(5L);
            trueNodeSet.add(7L);

            assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
            assertThat(edgeSet.size()).isEqualTo(0);
        }
    }

}
