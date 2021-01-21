package neo4j.cs;

import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.ServerPoliciesPlugin;

import java.util.Optional;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.LogProvider;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.virtual.MapValue;


import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;

@ServiceProvider
public class AllowReadsOnLeader extends ServerPoliciesPlugin
{
    @ServiceProvider
    public static class ServerSideRoutingPluginConfiguration implements SettingsDeclaration
    {
        //  reuse causal_clustering.cluster_allow_reads_on_leader (only available since 4.2)
        @Description( "List of addresses or address patterns that allow client side routing" )
        public static Setting<Boolean> allow_reads_on_leader =
                newBuilder( "causal_clustering.cluster_allow_reads_on_leader", BOOL , Boolean.FALSE ).dynamic().build();
    }

    private Config config;
    private TopologyService topologyService;
    private LeaderService leaderService;
    @Override
    public void init( TopologyService topologyService, LeaderService leaderService, LogProvider logProvider, Config config )
    {
        super.init( topologyService, leaderService, logProvider, config );
        this.config = config;
        this.topologyService = topologyService;
        this.leaderService = leaderService ;
    }

    private boolean isAllowed(  ) {
        return config.get( ServerSideRoutingPluginConfiguration.allow_reads_on_leader ).booleanValue();
    }

    @Override
    public String pluginName()
    {
        return "allow-reads-on-leader";
    }

    @Override
    public RoutingResult run( NamedDatabaseId namedDatabaseId, MapValue routingContext ) throws ProcedureException
    {
        RoutingResult stdResult= super.run(namedDatabaseId, routingContext);
        if (!isAllowed()) {
            return stdResult;
        }
        Optional<SocketAddress> leaderAddress = leaderService.getLeaderBoltAddress(namedDatabaseId);
        leaderAddress.ifPresent( address ->   stdResult.readEndpoints().add(address) );

        return stdResult;
    }
}