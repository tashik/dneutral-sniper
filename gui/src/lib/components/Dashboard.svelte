<script lang="ts">
  import { onMount } from 'svelte';
  import { createChart, type IChartApi, type ISeriesApi, type LineData } from 'lightweight-charts';
  import { portfolioStore } from '$lib/stores/portfolioStore';
  import type { Portfolio, OptionPosition } from '$types/portfolio';
  
  // Constants
  const MAX_RECONNECT_ATTEMPTS = 5;
  const RECONNECT_DELAY = 5000; // 5 seconds
  const PING_INTERVAL = 25000; // 25 seconds
  
  // WebSocket state
  let socket: WebSocket | null = null;
  let pingInterval: number | null = null;
  let reconnectAttempts = 0;
  let reconnectTimeout: number | null = null;
  let isConnected = false;
  
  // Chart state
  let chart: IChartApi | null = null;
  let lineSeries: ISeriesApi<'Line'> | null = null;
  let resizeObserver: ResizeObserver | null = null;
  let pnlData: LineData[] = [];
  
  // Component state
  let currentPnl = 0;
  let currentPortfolio: Portfolio | null = null;
  let portfolios: Portfolio[] = [];
  
  // DOM refs
  let chartContainer: HTMLElement | null = null;
  
  // Chart options
  const chartOptions = {
    layout: {
      background: { type: 'solid', color: '#ffffff' },
      textColor: '#333',
    },
    width: 0, // Will be set in initChart
    height: 400,
    grid: {
      vertLines: { color: '#f0f3fa' },
      horzLines: { color: '#f0f3fa' },
    },
    rightPriceScale: {
      borderColor: '#e0e3eb',
    },
    timeScale: {
      borderColor: '#e0e3eb',
    },
  };

  // Line series options
  const lineSeriesOptions = {
    color: '#3b82f6',
    lineWidth: 2,
    priceLineVisible: true,
    lastValueVisible: true,
    priceLineColor: '#3b82f6',
    priceLineWidth: 1,
  };
  
  // Calculate total value of a portfolio based on options
  function calculateTotalValue(portfolio: Portfolio): number {
    if (!portfolio?.options?.length) return 0;
    return portfolio.options.reduce<number>(
      (sum: number, option: OptionPosition) => {
        // Calculate value based on premium and quantity
        const optionValue = (option.premium || 0) * option.quantity;
        return sum + optionValue;
      },
      0
    );
  }

  // Store subscriptions
  $: {
    const store = $portfolioStore;
    currentPortfolio = store.currentPortfolio;
    portfolios = store.portfolios;
    
    if (store.currentPortfolio) {
      currentPnl = calculateTotalValue(store.currentPortfolio);
    }
  }

  // WebSocket message handler
  function handleWebSocketMessage(event: MessageEvent) {
    try {
      const data = JSON.parse(event.data);
      console.log('WebSocket message received:', data);

      // Handle different message types
      switch (data.type) {
        case 'pong':
          console.log('Received pong:', data.timestamp);
          break;

        case 'portfolio_updated':
          console.log('Portfolio updated:', data);
          // If the updated portfolio is the current one, update the store
          if (currentPortfolio && data.portfolio_id === currentPortfolio.id) {
            portfolioStore.setCurrentPortfolio(data.data);
          }
          break;

        case 'portfolio_deleted':
          console.log('Portfolio deleted:', data.portfolio_id);
          // If the deleted portfolio is the current one, clear the current portfolio
          if (currentPortfolio && data.portfolio_id === currentPortfolio.id) {
            portfolioStore.setCurrentPortfolio(null);
          }
          break;
          
        case 'pnl_update':
          console.log('Received PnL update:', data);
          if (data.portfolio_id === currentPortfolio?.id) {
            // Update chart with real PnL data
            updateChartWithRealData(data.history);
          }
          break;

        default:
          console.warn('Unknown WebSocket message type:', data.type);
      }
    } catch (error) {
      console.error('Error processing WebSocket message:', error);
    }
  }

  // Setup ping interval to keep connection alive
  function setupPing() {
    if (pingInterval) {
      clearInterval(pingInterval);
    }

    pingInterval = window.setInterval(() => {
      if (socket?.readyState === WebSocket.OPEN) {
        try {
          socket.send(JSON.stringify({ type: 'ping' }));
        } catch (error) {
          console.error('Error sending ping:', error);
        }
      } else if (pingInterval) {
        clearInterval(pingInterval);
        pingInterval = null;
      }
    }, PING_INTERVAL);
  }

  // WebSocket cleanup function
  function cleanupWebSocket() {
    if (socket) {
      try {
        if (socket.readyState === WebSocket.OPEN) {
          socket.close();
        }
        socket.onopen = null;
        socket.onmessage = null;
        socket.onclose = null;
        socket.onerror = null;
      } catch (error) {
        console.error('Error closing WebSocket:', error);
      } finally {
        socket = null;
      }
    }
    if (reconnectTimeout) {
      clearTimeout(reconnectTimeout);
      reconnectTimeout = null;
    }
  }

  // WebSocket reconnection logic
  function scheduleReconnect() {
    if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
      reconnectAttempts++;
      console.log(`Attempting to reconnect (${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})...`);
      reconnectTimeout = window.setTimeout(initWebSocket, RECONNECT_DELAY);
    } else {
      console.error('Max reconnection attempts reached. Please refresh the page.');
    }
  }

  // Initialize WebSocket connection
  function initWebSocket() {
    // Clean up any existing connection
    cleanupWebSocket();

    // Use WebSocket protocol based on current protocol (http/ws or https/wss)
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.hostname}:8000/ws`;

    try {
      socket = new WebSocket(wsUrl);
      console.log('WebSocket connecting to:', wsUrl);

      socket.onopen = () => {
        console.log('Connected to WebSocket');
        isConnected = true;
        reconnectAttempts = 0; // Reset reconnect attempts on successful connection
        setupPing(); // Start ping interval after connection is established

        // Send initial ping to verify connection
        if (socket && socket.readyState === WebSocket.OPEN) {
          try {
            socket.send(JSON.stringify({ type: 'ping' }));
          } catch (error) {
            console.error('Error sending initial ping:', error);
          }
        }
      };

      socket.onmessage = handleWebSocketMessage;

      socket.onclose = (event: CloseEvent) => {
        console.log('WebSocket connection closed:', event.code, event.reason);
        isConnected = false;
        
        // Only attempt to reconnect if this wasn't an intentional closure
        if (event.code !== 1000) { // 1000 = Normal closure
          scheduleReconnect();
        }
      };

      socket.onerror = (error: Event) => {
        console.error('WebSocket error:', error);
        isConnected = false;
      };
    } catch (error) {
      console.error('Failed to create WebSocket:', error);
      scheduleReconnect();
    }
  }

  onMount(() => {
    // Initialize the portfolio store and fetch portfolios
    portfolioStore.init();
    
    // Initialize chart and WebSocket
    if (chartContainer) {
      initChart();
      updateChartWithMockData();
    }
    initWebSocket();
    
    // Set up window resize handler
    window.addEventListener('resize', handleResize);
    
    // Cleanup function
    return () => {
      cleanupWebSocket();
      if (pingInterval) {
        clearInterval(pingInterval);
        pingInterval = null;
      }
      if (resizeObserver) {
        resizeObserver.disconnect();
        resizeObserver = null;
      }
      if (chart) {
        chart.remove();
        chart = null;
      }
      window.removeEventListener('resize', handleResize);
    };
  });

  // Update chart with real PnL data from WebSocket
  function updateChartWithRealData(history: Array<{timestamp: string; pnl: number}>) {
    if (!history || !history.length) return;
    
    // Convert history data to chart format with string timestamps
    const chartData = history.map(item => ({
      time: new Date(item.timestamp).toISOString().split('T')[0], // Format as YYYY-MM-DD
      value: item.pnl
    }));
    
    // Update chart data
    pnlData = chartData;
    
    // Update chart if it exists
    if (lineSeries && chart) {
      try {
        lineSeries.setData(chartData);
        chart.timeScale().fitContent();
      } catch (error) {
        console.error('Error updating chart with real data:', error);
      }
    }
  }
  
  // Mock data for the chart (fallback when no real data is available)
  function updateChartWithMockData() {
    const now = new Date();
    const mockData: LineData[] = [];

    // Generate 30 days of mock data
    for (let i = 30; i >= 0; i--) {
      const date = new Date(now);
      date.setDate(now.getDate() - i);

      mockData.push({
        time: date.toISOString().split('T')[0], // Format as YYYY-MM-DD
        value: 10000 + Math.random() * 2000 - 1000 + i * 100
      });
    }
    
    // Update chart data
    pnlData = mockData;

    // Update chart if it exists
    if (lineSeries && chart) {
      try {
        lineSeries.setData(pnlData);
        chart.timeScale().fitContent();
      } catch (error) {
        console.error('Error updating chart with mock data:', error);
      }
    }
  }

  // Initialize chart
  function initChart(): void {
    if (!chartContainer) {
      console.error('Chart container not found');
      return;
    }
    
    // Clean up existing chart if it exists
    if (chart) {
      chart.remove();
      chart = null;
      lineSeries = null;
    }
    
    try {
      // Create chart with options
      chart = createChart(chartContainer, {
        ...chartOptions,
        width: chartContainer.clientWidth,
        height: 400,
      });

      // Add line series with proper type
      lineSeries = chart.addSeries('Line', {
        color: lineSeriesOptions.color,
        lineWidth: lineSeriesOptions.lineWidth,
        priceLineVisible: lineSeriesOptions.priceLineVisible,
        lastValueVisible: lineSeriesOptions.lastValueVisible,
        priceLineColor: lineSeriesOptions.priceLineColor,
        priceLineWidth: lineSeriesOptions.priceLineWidth,
      });
      
      // Set initial data
      updateChartWithMockData();

      // Handle window resize
      resizeObserver = new ResizeObserver(entries => {
        if (!chart || entries.length === 0 || entries[0].target !== chartContainer) {
          return;
        }
        const { width } = entries[0].contentRect;
        chart.applyOptions({ width });
      });

      resizeObserver.observe(chartContainer);
      
    } catch (error) {
      console.error('Failed to initialize chart:', error);
    }
  }
  
  


  // Format currency
  function formatCurrency(value: number): string {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    }).format(value);
  }

  // Format date
  function formatDate(dateString: string | Date): string {
    if (!dateString) return 'N/A';
    const date = typeof dateString === 'string' ? new Date(dateString) : dateString;
    return date.toLocaleDateString();
  }

  // Calculate total premium for the current portfolio
  function calculateTotalPremium(): number {
    if (!currentPortfolio) return 0;
    return calculateTotalValue(currentPortfolio);
  }

  // Handle portfolio selection change
  function handlePortfolioChange(event: Event) {
    const select = event.target as HTMLSelectElement;
    const portfolioId = select.value;
    currentPortfolio = portfolios.find(p => p.id === portfolioId) || null;
    
    if (currentPortfolio) {
      updateChartWithMockData();
    }
  }

  // Handle window resize
  function handleResize() {
    if (chart && chartContainer) {
      chart.applyOptions({
        width: chartContainer.clientWidth,
      });
      chart.timeScale().fitContent();
    }
  }
</script>

<div class="dashboard">
  <div class="dashboard-header">
    <h2>Portfolio Dashboard</h2>
    <div class="portfolio-selector">
      <select
        value={currentPortfolio?.id || ''}
        on:change={handlePortfolioChange}
        class="select"
      >
        <option value="">Select a portfolio</option>
        {#each portfolios as portfolio (portfolio.id)}
          <option value={portfolio.id}>
            {portfolio.name}
          </option>
        {/each}
      </select>
    </div>
  </div>

  {#if currentPortfolio}
    <div class="portfolio-summary">
      <div class="summary-card">
        <div class="summary-label">Portfolio Value</div>
        <div class="summary-value">{formatCurrency(calculateTotalValue(currentPortfolio))}</div>
      </div>
      <div class="summary-card">
        <div class="summary-label">Total Premium</div>
        <div class="summary-value">{formatCurrency(calculateTotalPremium())}</div>
      </div>
      <div class="summary-card">
        <div class="summary-label">Options Count</div>
        <div class="summary-value">{currentPortfolio.options.length}</div>
      </div>
      <div class="summary-card">
        <div class="summary-label">Last Updated</div>
        <div class="summary-value">{formatDate(currentPortfolio.updated_at)}</div>
      </div>
    </div>

    <div class="chart-section">
      <h3>Portfolio Performance</h3>
      <div class="chart-container" bind:this={chartContainer}></div>
    </div>

    <div class="recent-positions">
      <div class="section-header">
        <h3>Recent Positions</h3>
        <button class="btn btn-sm btn-primary">View All</button>
      </div>

      {#if currentPortfolio.options.length > 0}
        <div class="positions-grid">
          {#each currentPortfolio.options.slice(0, 4) as option (option.id)}
            <div class="position-card">
              <div class="position-header">
                <span class="symbol">{option.symbol}</span>
                <span class={`type ${option.option_type}`}>
                  {option.option_type.toUpperCase()}
                </span>
              </div>
              <div class="position-details">
                <div class="detail">
                  <span class="label">Strike:</span>
                  <span class="value">${option.strike.toFixed(2)}</span>
                </div>
                <div class="detail">
                  <span class="label">Expiry:</span>
                  <span class="value">{formatDate(option.expiration)}</span>
                </div>
                <div class="detail">
                  <span class="label">Qty:</span>
                  <span class="value">{option.quantity}</span>
                </div>
                <div class="detail">
                  <span class="label">Premium:</span>
                  <span class="value">${(option.premium || 0).toFixed(2)}</span>
                </div>
              </div>
            </div>
          {/each}
        </div>
      {:else}
        <div class="empty-state">
          <p>No options positions found. Add options to this portfolio to see them here.</p>
          <button
            class="btn btn-primary"
            on:click={() => {
              // This would navigate to the options tab in a real app
              window.dispatchEvent(new CustomEvent('navigate', { detail: 'options' }));
            }}
          >
            Add Options
          </button>
        </div>
      {/if}
    </div>
  {:else}
    <div class="empty-dashboard">
      <div class="empty-content">
        <h3>Welcome to Options Portfolio Manager</h3>
        <p>Select a portfolio from the dropdown above or create a new one to get started.</p>
        {#if portfolios.length === 0}
          <button
            class="btn btn-primary"
            on:click={() => {
              // This would navigate to the portfolios tab in a real app
              window.dispatchEvent(new CustomEvent('navigate', { detail: 'portfolios' }));
            }}
          >
            Create Your First Portfolio
          </button>
        {/if}
      </div>
    </div>
  {/if}

  <div class="status-bar">
    <div class="status-item">
      <span class="label">Connection Status:</span>
      <span class="value {isConnected ? 'connected' : 'disconnected'}">
        {isConnected ? 'Connected' : 'Disconnected'}
      </span>
    </div>
    <div class="status-item">
      <span class="label">Last Update:</span>
      <span class="value">{new Date().toLocaleTimeString()}</span>
    </div>
  </div>
</div>

<style>
  .dashboard {
    max-width: 1200px;
    margin: 0 auto;
    padding: 20px;
  }

  .dashboard-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 24px;
  }

  .portfolio-selector {
    min-width: 250px;
  }

  .select {
    width: 100%;
    padding: 8px 12px;
    border: 1px solid #e2e8f0;
    border-radius: 4px;
    background-color: white;
    font-size: 14px;
    color: #1a202c;
  }

  .portfolio-summary {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
  }

  .summary-card {
    background: white;
    border-radius: 8px;
    padding: 16px;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  }

  .summary-label {
    font-size: 14px;
    color: #64748b;
    margin-bottom: 8px;
  }

  .summary-value {
    font-size: 20px;
    font-weight: 600;
    color: #1e293b;
  }

  .chart-section {
    background: white;
    border-radius: 8px;
    padding: 20px;
    margin-bottom: 24px;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  }

  .chart-section h3 {
    margin-top: 0;
    margin-bottom: 16px;
    color: #1e293b;
  }

  .chart-container {
    width: 100%;
    height: 400px;
    border-radius: 4px;
    overflow: hidden;
  }

  .recent-positions {
    background: white;
    border-radius: 8px;
    padding: 20px;
    margin-bottom: 24px;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  }

  .section-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
  }

  .section-header h3 {
    margin: 0;
    color: #1e293b;
  }

  .status-indicator.disconnected {
    color: #e74c3c;
  }

  .positions, .pnl {
    margin: 20px 0;
    padding: 15px;
    background: #f9f9f9;
    border-radius: 4px;
  }

  h2 {
    color: #2c3e50;
  }
</style>
