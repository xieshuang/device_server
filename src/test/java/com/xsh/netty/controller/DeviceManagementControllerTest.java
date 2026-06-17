package com.xsh.netty.controller;

import com.xsh.netty.auth.DeviceRevocationService;
import com.xsh.netty.server.DeviceChannelManager;
import com.xsh.netty.server.DeviceSession;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * DeviceManagementController 集成测试。
 */
@WebMvcTest(DeviceManagementController.class)
class DeviceManagementControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private DeviceChannelManager channelManager;

    @MockBean
    private DeviceRevocationService revocationService;

    @Test
    void testGetOnlineDevices() throws Exception {
        ConcurrentHashMap<String, DeviceSession> sessions = new ConcurrentHashMap<>();
        DeviceSession session = new DeviceSession("dev-001", new EmbeddedChannel(), Instant.now());
        sessions.put("dev-001", session);
        when(channelManager.getOnlineDevices()).thenReturn(sessions);

        mockMvc.perform(get("/api/devices/online"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].deviceId").value("dev-001"));
    }

    @Test
    void testGetOnlineDevices_Empty() throws Exception {
        when(channelManager.getOnlineDevices()).thenReturn(Map.of());

        mockMvc.perform(get("/api/devices/online"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(0)));
    }

    @Test
    void testGetDevice_Online() throws Exception {
        when(channelManager.getChannel("dev-001")).thenReturn(new EmbeddedChannel());
        when(revocationService.isRevoked("dev-001")).thenReturn(false);

        mockMvc.perform(get("/api/devices/dev-001"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.deviceId").value("dev-001"))
                .andExpect(jsonPath("$.online").value(true))
                .andExpect(jsonPath("$.revoked").value(false));
    }

    @Test
    void testGetDevice_OfflineAndRevoked() throws Exception {
        when(channelManager.getChannel("dev-002")).thenReturn(null);
        when(revocationService.isRevoked("dev-002")).thenReturn(true);

        mockMvc.perform(get("/api/devices/dev-002"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.online").value(false))
                .andExpect(jsonPath("$.revoked").value(true));
    }

    @Test
    void testForceOffline() throws Exception {
        when(channelManager.sendToDevice(eq("dev-001"), any())).thenReturn(true);

        mockMvc.perform(delete("/api/devices/dev-001"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));
    }

    @Test
    void testRevokeDevice() throws Exception {
        doNothing().when(revocationService).revoke(anyString(), anyLong());

        mockMvc.perform(post("/api/devices/dev-001/revoke").param("ttlSeconds", "3600"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.revoked").value(true))
                .andExpect(jsonPath("$.ttlSeconds").value(3600));
    }

    @Test
    void testUnrevokeDevice() throws Exception {
        doNothing().when(revocationService).unrevoke("dev-001");

        mockMvc.perform(delete("/api/devices/dev-001/revoke"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.revoked").value(false));
    }

    @Test
    void testGetStats() throws Exception {
        when(channelManager.getOnlineCount()).thenReturn(3);
        when(channelManager.getOnlineDevices()).thenReturn(Map.of(
                "a", mock(DeviceSession.class),
                "b", mock(DeviceSession.class),
                "c", mock(DeviceSession.class)
        ));

        mockMvc.perform(get("/api/devices/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.onlineCount").value(3))
                .andExpect(jsonPath("$.totalDevices").value(3));
    }
}
