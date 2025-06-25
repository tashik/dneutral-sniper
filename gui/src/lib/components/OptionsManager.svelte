<script lang="ts">
  import { onMount } from 'svelte';
  import { portfolioStore } from '../stores/portfolioStore';
  import type { OptionPosition } from '../../types/portfolio';

  // Form state
  let showForm = false;
  let formData: Omit<OptionPosition, 'id' | 'created_at' | 'updated_at'> = {
    symbol: '',
    option_type: 'call',
    strike: 0,
    expiration: new Date().toISOString().split('T')[0],
    quantity: 1,
    premium: 0
  };

  // Get current portfolio from store
  $: currentPortfolio = $portfolioStore.currentPortfolio;

  // Form handlers
  function handleAddClick() {
    formData = {
      symbol: '',
      option_type: 'call',
      strike: 0,
      expiration: new Date().toISOString().split('T')[0],
      quantity: 1,
      premium: 0
    };
    showForm = true;
  }

  async function handleSubmit() {
    if (!currentPortfolio) return;

    try {
      await portfolioStore.addOption(currentPortfolio.id, {
        ...formData,
        expiration: new Date(formData.expiration).toISOString()
      });
      showForm = false;
    } catch (error) {
      console.error('Error adding option:', error);
    }
  }

  async function handleDelete(optionId: string) {
    if (!currentPortfolio) return;

    if (confirm('Are you sure you want to remove this option?')) {
      try {
        await portfolioStore.removeOption(currentPortfolio.id, optionId);
      } catch (error) {
        console.error('Error removing option:', error);
      }
    }
  }

  function formatDate(dateString: string) {
    return new Date(dateString).toLocaleDateString();
  }

  function calculatePremium(option: OptionPosition) {
    return option.premium * option.quantity * 100; // Assuming premium is per share
  }

  function calculateStrikeValue(option: OptionPosition) {
    return option.strike * option.quantity * 100; // Assuming 100 shares per contract
  }
</script>

{#if !currentPortfolio}
  <div class="no-portfolio">
    <p>Please select a portfolio to view or add options.</p>
  </div>
{:else}
  <div class="options-manager">
    <div class="header">
      <h3>{currentPortfolio.name} - Options</h3>
      <button on:click={handleAddClick} class="btn btn-primary">
        Add Option
      </button>
    </div>

    {#if showForm}
      <div class="form-container">
        <h4>Add New Option</h4>
        <div class="form-grid">
          <div class="form-group">
            <label for="symbol">Symbol</label>
            <input
              type="text"
              id="symbol"
              bind:value={formData.symbol}
              class="form-control"
              placeholder="e.g., BTC"
              required
            />
          </div>

          <div class="form-group">
            <label for="option_type">Type</label>
            <select
              id="option_type"
              bind:value={formData.option_type}
              class="form-control"
            >
              <option value="call">Call</option>
              <option value="put">Put</option>
            </select>
          </div>

          <div class="form-group">
            <label for="strike">Strike Price</label>
            <input
              type="number"
              id="strike"
              bind:value={formData.strike}
              class="form-control"
              min="0"
              step="0.01"
              required
            />
          </div>

          <div class="form-group">
            <label for="expiration">Expiration</label>
            <input
              type="date"
              id="expiration"
              bind:value={formData.expiration}
              class="form-control"
              required
            />
          </div>

          <div class="form-group">
            <label for="quantity">Quantity</label>
            <input
              type="number"
              id="quantity"
              bind:value={formData.quantity}
              class="form-control"
              min="1"
              required
            />
          </div>

          <div class="form-group">
            <label for="premium">Premium</label>
            <input
              type="number"
              id="premium"
              bind:value={formData.premium}
              class="form-control"
              min="0"
              step="0.01"
              required
            />
          </div>
        </div>

        <div class="form-actions">
          <button on:click|preventDefault={() => showForm = false} class="btn btn-secondary">
            Cancel
          </button>
          <button on:click|preventDefault={handleSubmit} class="btn btn-primary">
            Add Option
          </button>
        </div>
      </div>
    {/if}

    <div class="options-list">
      {#if currentPortfolio.options.length === 0}
        <div class="empty">No options found. Add your first option to get started!</div>
      {:else}
        <table class="table">
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Type</th>
              <th>Strike</th>
              <th>Expiration</th>
              <th>Quantity</th>
              <th>Premium</th>
              <th>Total Premium</th>
              <th>Strike Value</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {#each currentPortfolio.options as option (option.id)}
              <tr>
                <td>{option.symbol}</td>
                <td class={option.option_type}>
                  {option.option_type.charAt(0).toUpperCase() + option.option_type.slice(1)}
                </td>
                <td>${option.strike.toFixed(2)}</td>
                <td>{formatDate(option.expiration)}</td>
                <td>{option.quantity}</td>
                <td>${option.premium.toFixed(2)}</td>
                <td>${calculatePremium(option).toFixed(2)}</td>
                <td>${calculateStrikeValue(option).toFixed(2)}</td>
                <td class="actions">
                  <button on:click|preventDefault={() => handleDelete(option.id)} class="btn btn-sm btn-delete">
                    Remove
                  </button>
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      {/if}
    </div>
  </div>
{/if}

<style>
  .options-manager {
    margin-top: 30px;
  }

  .no-portfolio {
    text-align: center;
    padding: 20px;
    color: #6c757d;
  }

  .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 20px;
  }

  .form-container {
    background: #f8f9fa;
    border-radius: 8px;
    padding: 20px;
    margin-bottom: 20px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
  }

  .form-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
    gap: 15px;
    margin-bottom: 15px;
  }

  .form-group {
    margin-bottom: 0;
  }

  label {
    display: block;
    margin-bottom: 5px;
    font-size: 0.9em;
    color: #495057;
  }

  .form-control {
    width: 100%;
    padding: 8px 12px;
    border: 1px solid #ced4da;
    border-radius: 4px;
    font-size: 14px;
  }

  .form-actions {
    display: flex;
    justify-content: flex-end;
    gap: 10px;
    margin-top: 15px;
  }

  .btn {
    padding: 8px 16px;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    font-size: 14px;
    transition: background-color 0.2s;
  }

  .btn-primary {
    background-color: #007bff;
    color: white;
  }

  .btn-primary:hover {
    background-color: #0056b3;
  }

  .btn-secondary {
    background-color: #6c757d;
    color: white;
  }

  .btn-secondary:hover {
    background-color: #5a6268;
  }

  .btn-sm {
    padding: 4px 8px;
    font-size: 12px;
  }

  .btn-delete {
    background-color: #dc3545;
    color: white;
  }

  .btn-delete:hover {
    background-color: #c82333;
  }

  .table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 15px;
    font-size: 0.9em;
  }

  .table th,
  .table td {
    padding: 12px 15px;
    text-align: left;
    border-bottom: 1px solid #dee2e6;
  }

  .table th {
    background-color: #f8f9fa;
    font-weight: 600;
  }

  .table tbody tr:hover {
    background-color: #f5f5f5;
  }

  .actions {
    display: flex;
    gap: 5px;
  }

  .empty {
    padding: 20px;
    text-align: center;
    color: #6c757d;
  }

  .call {
    color: #28a745;
    font-weight: 500;
  }

  .put {
    color: #dc3545;
    font-weight: 500;
  }
</style>
