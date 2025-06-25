<script lang="ts">
  import { onMount } from 'svelte';
  import { portfolioStore } from './lib/stores/portfolioStore';

  // Import components
  import PortfolioManager from './lib/components/PortfolioManager.svelte';
  import OptionsManager from './lib/components/OptionsManager.svelte';
  import Dashboard from './lib/components/Dashboard.svelte';

  // Track current view
  let currentView = 'dashboard';

  // Initialize store
  onMount(() => {
    portfolioStore.init();
  });
</script>

<div class="app-container">
  <header class="app-header">
    <h1>Options Portfolio Manager</h1>
    <nav class="nav-tabs">
      <button
        class="tab {currentView === 'dashboard' ? 'active' : ''}"
        on:click={() => currentView = 'dashboard'}
      >
        Dashboard
      </button>
      <button
        class="tab {currentView === 'portfolios' ? 'active' : ''}"
        on:click={() => currentView = 'portfolios'}
      >
        Portfolios
      </button>
      <button
        class="tab {currentView === 'options' ? 'active' : ''}"
        on:click={() => currentView = 'options'}
      >
        Options
      </button>
    </nav>
  </header>

  <main class="app-content">
    {#if currentView === 'dashboard'}
      <Dashboard />
    {:else if currentView === 'portfolios'}
      <PortfolioManager />
    {:else if currentView === 'options'}
      <OptionsManager />
    {/if}
  </main>

  <footer class="app-footer">
    <p>© {new Date().getFullYear()} Options Portfolio Manager</p>
  </footer>
</div>

<style>
  :global(*) {
    box-sizing: border-box;
    margin: 0;
    padding: 0;
  }

  :global(body) {
    margin: 0;
    padding: 0;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
    background-color: #f5f7fa;
    color: #333;
    line-height: 1.6;
  }

  .app-container {
    display: flex;
    flex-direction: column;
    min-height: 100vh;
  }

  .app-header {
    background-color: #2c3e50;
    color: white;
    padding: 1rem 2rem;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
  }

  .app-header h1 {
    margin: 0 0 1rem 0;
    font-size: 1.8rem;
  }

  .nav-tabs {
    display: flex;
    gap: 1rem;
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    padding-bottom: 0.5rem;
  }

  .tab {
    background: none;
    border: none;
    color: rgba(255, 255, 255, 0.7);
    padding: 0.5rem 1rem;
    cursor: pointer;
    font-size: 1rem;
    border-radius: 4px;
    transition: all 0.2s;
  }

  .tab:hover {
    background-color: rgba(255, 255, 255, 0.1);
    color: white;
  }

  .tab.active {
    background-color: #3498db;
    color: white;
  }

  .app-content {
    flex: 1;
    padding: 2rem;
    max-width: 1400px;
    width: 100%;
    margin: 0 auto;
  }

  .app-footer {
    background-color: #2c3e50;
    color: white;
    text-align: center;
    padding: 1rem;
    margin-top: auto;
  }

  .app-footer p {
    margin: 0;
    font-size: 0.9rem;
    color: rgba(255, 255, 255, 0.7);
  }

  /* Responsive design */
  @media (max-width: 768px) {
    .app-header {
      padding: 0.75rem 1rem;
    }

    .app-header h1 {
      font-size: 1.5rem;
      margin-bottom: 0.75rem;
    }

    .nav-tabs {
      overflow-x: auto;
      padding-bottom: 0.25rem;
      -webkit-overflow-scrolling: touch;
    }

    .app-content {
      padding: 1rem;
    }
  }
</style>
