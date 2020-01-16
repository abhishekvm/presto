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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;

/**
 * To get custom properties in order to connect to the database.
 * User, password and URL parameters are provided by BaseJdbcClient; and are not required.
 * If there is another custom configuration it should be put here.
 */
public class RocksetConfig
{
    private String apiKey;
    private String apiServer;

    public String getApiKey()
    {
        return apiKey;
    }

    @Config("connection-api-key")
    public RocksetConfig setApiKey(String apiKey)
    {
        this.apiKey = apiKey;
        return this;
    }

    public String getApiServer()
    {
        return apiServer;
    }

    @Config("connection-api-server")
    @ConfigSecuritySensitive
    public RocksetConfig setApiServer(String apiServer)
    {
        this.apiServer = apiServer;
        return this;
    }
}
