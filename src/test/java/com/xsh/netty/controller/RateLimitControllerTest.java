package com.xsh.netty.controller;

import com.xsh.netty.ratelimit.RateLimiterService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * RateLimitController 集成测试。
 */
@WebMvcTest(RateLimitController.class)
class RateLimitControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private RateLimiterService rateLimiterService;

    @Test
    void testSetGlobalRate() throws Exception {
        doNothing().when(rateLimiterService).setGlobalRate(5000.0);

        mockMvc.perform(put("/api/ratelimit/global").param("permits", "5000"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.globalPermitsPerSecond").value(5000.0));
    }

    @Test
    void testSetDeviceRate() throws Exception {
        doNothing().when(rateLimiterService).setDeviceRate("dev-001", 200.0);

        mockMvc.perform(put("/api/ratelimit/device/dev-001").param("permits", "200"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.deviceId").value("dev-001"))
                .andExpect(jsonPath("$.permitsPerSecond").value(200.0));
    }
}
