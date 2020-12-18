package org.apache.incubator.wayang.graphchi.plugin;

import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.mapping.Mapping;
import org.apache.incubator.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.incubator.wayang.core.platform.Platform;
import org.apache.incubator.wayang.core.plugin.Plugin;
import org.apache.incubator.wayang.core.util.fs.LocalFileSystem;
import org.apache.incubator.wayang.graphchi.channels.ChannelConversions;
import org.apache.incubator.wayang.graphchi.mappings.Mappings;
import org.apache.incubator.wayang.graphchi.platform.GraphChiPlatform;

import java.io.File;
import java.util.Collection;
import java.util.Collections;

/**
 * This {@link Plugin} activates default capabilities of the {@link GraphChiPlatform}.
 */
public class GraphChiPlugin implements Plugin {

    @Override
    public Collection<Mapping> getMappings() {
        return Mappings.ALL;
    }

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Collections.singleton(GraphChiPlatform.getInstance());
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return ChannelConversions.ALL;
    }

    @Override
    public void setProperties(Configuration configuration) {
        final File localTempDir = LocalFileSystem.findTempDir();
        if (localTempDir != null) {
            configuration.setProperty("wayang.graphchi.tempdir", localTempDir.toString());
        }
    }

}