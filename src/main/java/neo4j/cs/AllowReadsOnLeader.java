package neo4j.cs;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.discovery.*;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.routing.Endpoint;
import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingPlugin;
import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingResult;
import org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.*;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.kernel.configuration.Config;

import org.neo4j.helpers.Service;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyList;
import static org.neo4j.causalclustering.routing.Util.asList;
import static org.neo4j.causalclustering.routing.Util.extractBoltAddress;



@Service.Implementation( LoadBalancingPlugin.class )
public class AllowReadsOnLeader implements LoadBalancingPlugin
{

    private Config config;
    private TopologyService topologyService;
    private LeaderLocator leaderLocator;
    private Long timeToLive;
    private Log log;
    private boolean shouldShuffle;
    private boolean allowReadsOnFollowers;

    @Override
    public void init( TopologyService topologyService, LeaderLocator leaderLocator, LogProvider logProvider, Config config )
            throws InvalidFilterSpecification
    {
        this.config = config;
        this.topologyService = topologyService;
        this.leaderLocator = leaderLocator ;
        this.log = logProvider.getLog(this.getClass());
        this.shouldShuffle = config.get( CausalClusteringSettings.load_balancing_shuffle );
        this.timeToLive = ((Duration)config.get(CausalClusteringSettings.cluster_routing_ttl)).toMillis();
        this.allowReadsOnFollowers = config.get( CausalClusteringSettings.cluster_allow_reads_on_followers );
    }

    @Override
    public void validate( Config config, Log log ) throws InvalidSettingException
    {
    }

    @Override
    public boolean isShufflingPlugin() {
        return true;
    }

    @Override
    public String pluginName()
    {
        return "allow-reads-on-leader";
    }

    private List<Endpoint> writeEndpoints( CoreTopology cores )
    {
        MemberId leader;
        try {
            leader = leaderLocator.getLeader();
        }
        catch ( NoLeaderFoundException e ) {
            return emptyList();
        }

        Optional<Endpoint> endPoint = cores.find( leader )
                .map( extractBoltAddress() )
                .map( Endpoint::write );

        return asList( endPoint );
    }

    private List<Endpoint> routeEndpoints( CoreTopology cores)
    {
        List<Endpoint> routers = cores.members().entrySet().stream()
                .map( e ->
                {
                    MemberId m = e.getKey();
                    CoreServerInfo c = e.getValue();
                    return Endpoint.route(c.connectors().boltAddress());
                } ).collect( Collectors.toList());

        if ( shouldShuffle ) { Collections.shuffle( routers ); }

        return routers;
    }

    private List<Endpoint> readEndpoints( CoreTopology coreTopology, ReadReplicaTopology rrTopology)
    {

        Set<Endpoint> possibleReaders = rrTopology.members().entrySet().stream()
                .map( entry -> Endpoint.read(entry.getValue().connectors().boltAddress()) )
                .collect( Collectors.toSet() );

        if ( allowReadsOnFollowers || possibleReaders.size() == 0 )
        {
            Set<MemberId> validCores = coreTopology.members().keySet();
            for ( MemberId validCore : validCores )
            {
                Optional<CoreServerInfo> coreServerInfo = coreTopology.find( validCore );
                coreServerInfo.ifPresent(
                        coreServerInfo1 -> possibleReaders.add(
                                Endpoint.read(coreServerInfo1.connectors().boltAddress()) ) );
            }
        }
        List<Endpoint> readers = new ArrayList<>(  possibleReaders  );

        if ( shouldShuffle ) { Collections.shuffle( readers ); }
        return readers;
    }

    @Override
    public Result run( Map<String,String> context ) throws ProcedureException
    {
        CoreTopology coreTopology = topologyService.localCoreServers();
        ReadReplicaTopology rrTopology = topologyService.localReadReplicas();

        return new LoadBalancingResult(routeEndpoints(coreTopology), writeEndpoints( coreTopology ),
                readEndpoints(coreTopology, rrTopology), this.timeToLive);
    }
}