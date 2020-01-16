/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rockset;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.rockset.jdbc.RocksetDriver;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * Implementation of TeradataClient. It describes table, schemas and columns behaviours.
 * It allows to change the QueryBuilder to a custom one as well.
 */
public class RocksetClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(BaseJdbcClient.class);

    @Inject
    public RocksetClient(JdbcConnectorId connectorId, BaseJdbcConfig config, RocksetConfig rocksetConfig)
            throws SQLException
    {
        /*
            We use an empty string as identifierQuote parameter, to avoid using quotes when creating queries
            The following properties are already set to BaseJdbcClient, via properties injection:
            - connectionProperties.setProperty("user", teradataConfig.getUser());
            - connectionProperties.setProperty("url", teradataConfig.getUrl());
            - connectionProperties.setProperty("password", teradataConfig.getPassword());
         */
        super(connectorId, config, "", connectionFactory(rocksetConfig));
    }

    private static ConnectionFactory connectionFactory(RocksetConfig rocksetConfig)
            throws SQLException
    {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("driver", "com.rockset.jdbc.RocksetDriver");
        connectionProperties.setProperty("apiKey", rocksetConfig.getApiKey());
        connectionProperties.setProperty("apiServer", rocksetConfig.getApiServer());

        return new DriverConnectionFactory(new RocksetDriver(), "jdbc:rockset://", connectionProperties);
    }

    @Nullable
    @Override
    public JdbcTableHandle getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String remoteSchema = toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());
            log.info("RemoteSchema " + remoteSchema);
            log.info("RemoteSchema " + remoteTable);
            try (ResultSet resultSet = getTables(connection, Optional.of(remoteSchema), Optional.of(remoteTable))) {
                Set<JdbcTableHandle> tableHandles = new HashSet<>();
                while (resultSet.next()) {
                    log.info("Catalog: " + resultSet.getString("TABLE_CAT"));
                    log.info("Schema: " + resultSet.getString("TABLE_SCHEM"));
                    log.info("Table Name " + resultSet.getString("TABLE_NAME"));

                    if (remoteTable.equals(resultSet.getString("TABLE_NAME"))) {
                        log.info("Found matching table");
                        tableHandles.add(new JdbcTableHandle(
                                connectorId,
                                schemaTableName,
                                resultSet.getString("TABLE_CAT"),
                                resultSet.getString("TABLE_SCHEM"),
                                resultSet.getString("TABLE_NAME")));
                    }
                }
                if (tableHandles.isEmpty()) {
                    return null;
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                JdbcTableHandle tableHandle = getOnlyElement(tableHandles);
                log.info("Table Handle " + tableHandle);
                return tableHandle;
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }
}
