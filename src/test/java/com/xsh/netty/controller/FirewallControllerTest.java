package com.xsh.netty.controller;

import com.xsh.netty.server.IpFirewallService;
import com.xsh.netty.server.NettyServerProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * FirewallController 集成测试。
 */
@WebMvcTest(FirewallController.class)
class FirewallControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private IpFirewallService firewallService;

    @MockBean
    private NettyServerProperties properties;

    @Test
    void testIsBanned_True() throws Exception {
        when(firewallService.isBanned("192.168.1.100")).thenReturn(true);

        mockMvc.perform(get("/api/firewall/banned/192.168.1.100"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ip").value("192.168.1.100"))
                .andExpect(jsonPath("$.banned").value(true));
    }

    @Test
    void testIsBanned_False() throws Exception {
        when(firewallService.isBanned("10.0.0.1")).thenReturn(false);

        mockMvc.perform(get("/api/firewall/banned/10.0.0.1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.banned").value(false));
    }

    @Test
    void testBanIp() throws Exception {
        when(properties.getIpFilterBanThreshold()).thenReturn(5);
        doNothing().when(firewallService).recordFailure(anyString());

        mockMvc.perform(post("/api/firewall/ban/192.168.1.100").param("minutes", "60"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.banned").value(true))
                .andExpect(jsonPath("$.durationMinutes").value(60));

        // 验证调用了阈值次数
        verify(firewallService, times(5)).recordFailure("192.168.1.100");
    }
}
