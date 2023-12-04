/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.datasource.starrocks.param;

import org.apache.dolphinscheduler.common.constants.DataSourceConstants;
import org.apache.dolphinscheduler.plugin.datasource.api.plugin.DataSourceClientProvider;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.CommonUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.DataSourceUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils;
import org.apache.dolphinscheduler.spi.enums.DbType;

import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Class.class, DriverManager.class, DataSourceUtils.class, CommonUtils.class,
        DataSourceClientProvider.class, PasswordUtils.class})
public class StarRocksDataSourceProcessorTest {

    private StarRocksDataSourceProcessor starrocksDatasourceProcessor = new StarRocksDataSourceProcessor();

    @Test
    public void testCreateConnectionParams() {
        Map<String, String> props = new HashMap<>();
        props.put("serverTimezone", "utc");
        StarRocksDataSourceParamDTO starrocksDatasourceParamDTO = new StarRocksDataSourceParamDTO();
        starrocksDatasourceParamDTO.setUserName("root");
        starrocksDatasourceParamDTO.setPassword("123456");
        starrocksDatasourceParamDTO.setHost("localhost");
        starrocksDatasourceParamDTO.setPort(3306);
        starrocksDatasourceParamDTO.setDatabase("default");
        starrocksDatasourceParamDTO.setOther(props);
        PowerMockito.mockStatic(PasswordUtils.class);
        PowerMockito.when(PasswordUtils.encodePassword(Mockito.anyString())).thenReturn("test");
        StarRocksConnectionParam connectionParams = (StarRocksConnectionParam) starrocksDatasourceProcessor
                .createConnectionParams(starrocksDatasourceParamDTO);
        Assert.assertEquals("jdbc:starrocks://localhost:3306", connectionParams.getAddress());
        Assert.assertEquals("jdbc:starrocks://localhost:3306/default", connectionParams.getJdbcUrl());
    }

    @Test
    public void testCreateConnectionParams2() {
        String connectionJson =
                "{\"user\":\"root\",\"password\":\"123456\",\"address\":\"jdbc:starrocks://localhost:3306\""
                        + ",\"database\":\"default\",\"jdbcUrl\":\"jdbc:starrocks://localhost:3306/default\"}";
        StarRocksConnectionParam connectionParams = (StarRocksConnectionParam) starrocksDatasourceProcessor
                .createConnectionParams(connectionJson);
        Assert.assertNotNull(connectionJson);
        Assert.assertEquals("root", connectionParams.getUser());
    }

    @Test
    public void testGetDatasourceDriver() {
        Assertions.assertEquals(DataSourceConstants.COM_MYSQL_CJ_JDBC_DRIVER,
                starrocksDatasourceProcessor.getDatasourceDriver());
    }

    @Test
    public void testGetJdbcUrl() {
        StarRocksConnectionParam starrocksConnectionParam = new StarRocksConnectionParam();
        starrocksConnectionParam.setJdbcUrl("jdbc:starrocks://localhost:3306/default");
        Assert.assertEquals(
                "jdbc:starrocks://localhost:3306/default?allowLoadLocalInfile=false&autoDeserialize=false&allowLocalInfile=false&allowUrlInLocalInfile=false",
                starrocksDatasourceProcessor.getJdbcUrl(starrocksConnectionParam));
    }

    @Test
    public void testGetDbType() {
        Assert.assertEquals(DbType.STARROCKS, starrocksDatasourceProcessor.getDbType());
    }

    @Test
    public void testGetValidationQuery() {
        Assertions.assertEquals(DataSourceConstants.MYSQL_VALIDATION_QUERY,
                starrocksDatasourceProcessor.getValidationQuery());
    }

    @Test
    public void testGetDatasourceUniqueId() {
        StarRocksConnectionParam starrocksConnectionParam = new StarRocksConnectionParam();
        starrocksConnectionParam.setJdbcUrl("jdbc:starrocks://localhost:3306/default");
        starrocksConnectionParam.setUser("root");
        starrocksConnectionParam.setPassword("123456");
        PowerMockito.mockStatic(PasswordUtils.class);
        PowerMockito.when(PasswordUtils.encodePassword(Mockito.anyString())).thenReturn("123456");
        Assert.assertEquals("starrocks@root@123456@jdbc:mysql://localhost:3306/default",
                starrocksDatasourceProcessor.getDatasourceUniqueId(starrocksConnectionParam, DbType.STARROCKS));
    }
}
