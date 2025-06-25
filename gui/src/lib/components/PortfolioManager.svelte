<script lang="ts">
  import { onMount } from 'svelte';
  import { portfolioStore } from '../stores/portfolioStore';
  import type { Portfolio, OptionPosition } from '$types/portfolio';
  
  // Store subscription
  $: portfolios = $portfolioStore.portfolios;
  $: currentPortfolio = $portfolioStore.currentPortfolio;
  $: loading = $portfolioStore.loading;
  $: error = $portfolioStore.error;

  // Track the currently selected portfolio ID
  let selectedPortfolioId: string | null = null;

  // Function to handle portfolio selection
  function selectPortfolio(portfolioId: string) {
    selectedPortfolioId = portfolioId === selectedPortfolioId ? null : portfolioId;

    // Set the current portfolio in the store
    if (selectedPortfolioId) {
      const portfolio = $portfolioStore.portfolios.find(p => p.id === selectedPortfolioId);
      if (portfolio) {
        portfolioStore.setCurrentPortfolio(portfolio);
      }
    } else {
      portfolioStore.setCurrentPortfolio(null);
    }
  }

  // Portfolio form state
  let showPortfolioForm = false;
  let isEditingPortfolio = false;

  // Portfolio form data
  interface PortfolioFormData {
    id: string;
    name: string;
    description: string;
    underlying: string;
  }
  
  let portfolioFormData: PortfolioFormData = {
    id: '',
    name: '',
    description: '',
    underlying: 'BTC-USD' // Default value
  };

  // Function to reset portfolio form data
  function resetPortfolioForm() {
    portfolioFormData = {
      id: '',
      name: '',
      description: '',
      underlying: 'BTC-USD' // Reset to default
    };
    isEditingPortfolio = false;
  }

  // Function to handle portfolio form submission
  async function handlePortfolioSubmit() {
    try {
      if (isEditingPortfolio && portfolioFormData.id) {
        await portfolioStore.updatePortfolio(
          portfolioFormData.id,
          portfolioFormData.name,
          portfolioFormData.underlying,
          portfolioFormData.description
        );
      } else {
        await portfolioStore.createPortfolio(
          portfolioFormData.name, 
          portfolioFormData.underlying, 
          portfolioFormData.description
        );
      }
      showPortfolioForm = false;
    } catch (error) {
      console.error('Error saving portfolio:', error);
      // TODO: Show error to user
    }
  }

  // Portfolio form handlers
  function handleAddPortfolioClick() {
    resetPortfolioForm();
    isEditingPortfolio = false;
    showPortfolioForm = true;
  }

  function handleEditPortfolio(portfolio: Portfolio) {
    isEditingPortfolio = true;
    portfolioFormData = {
      id: portfolio.id,
      name: portfolio.name,
      description: portfolio.description || '',
      underlying: portfolio.underlying || 'BTC-USD'
    };
    showPortfolioForm = true;
  }

  // Option form state
  let showOptionForm = false;
  let selectedPortfolioIdForOption: string | null = null;
  
  interface OptionFormData {
    underlying: string;
    expiry: string;
    strike: string;
    optionType: 'call' | 'put';
    contractType: 'STANDARD' | 'INVERSE';
    quantity: number;
    premium: string;
    instrumentName: string;
  }
  
  let optionFormData: OptionFormData = {
    underlying: 'BTC', // Default, will be overridden by portfolio's underlying
    expiry: '',
    strike: '',
    optionType: 'call',
    contractType: 'STANDARD',
    quantity: 1,
    premium: '0',
    instrumentName: ''
  };
  
  // Calculate instrument name when relevant fields change
  $: {
    if (optionFormData.underlying && optionFormData.expiry && optionFormData.strike && optionFormData.optionType) {
      try {
        const expiryDate = new Date(optionFormData.expiry);
        if (isNaN(expiryDate.getTime())) {
          optionFormData = { ...optionFormData, instrumentName: '' };
        } else {
          const expiryStr = expiryDate.toISOString().split('T')[0].replace(/-/g, '');
          const strikePadded = parseInt(optionFormData.strike).toString().padStart(8, '0');
          optionFormData = {
            ...optionFormData,
            instrumentName: `${optionFormData.underlying}-${expiryStr}-${strikePadded}-${optionFormData.optionType.toUpperCase()}`
          };
        }
      } catch (e) {
        optionFormData = { ...optionFormData, instrumentName: '' };
      }
    } else {
      optionFormData = {
        ...optionFormData,
        instrumentName: ''
      };
    }
  }

  // Portfolio table columns
  const portfolioColumns = [
    { key: 'name', label: 'Name' },
    { key: 'underlying', label: 'Underlying' },
    { key: 'description', label: 'Description' },
    { key: 'created_at', label: 'Created' },
    { key: 'updated_at', label: 'Updated' },
    { key: 'options', label: 'Options' },
    { key: 'actions', label: 'Actions' }
  ];

  // Initialize store
  onMount(() => {
    portfolioStore.init();
  });

  // Handle add option click
  function handleAddOptionClick(portfolio: Portfolio) {
    selectedPortfolioIdForOption = portfolio.id;
    // Use the portfolio's underlying or extract from name if not set
    const defaultUnderlying = portfolio.underlying || 
      (portfolio.name.match(/\b(BTC|ETH|SOL|\w+)\b/)?.[1]?.toUpperCase() || 'BTC');
    
    optionFormData = {
      ...optionFormData,
      underlying: defaultUnderlying,
      expiry: '',
      strike: '',
      optionType: 'call',
      contractType: 'STANDARD',
      quantity: 1,
      premium: '0',
      instrumentName: ''
    };
    showOptionForm = true;
  }

  function handleOptionSubmit() {
    // TODO: Implement actual API call to save the option
    console.log('Saving option:', {
      ...optionFormData,
      portfolioId: selectedPortfolioIdForOption
    });

    // Reset form
    showOptionForm = false;
    selectedPortfolioIdForOption = null;
  }

  async function handleDelete(id: string) {
    if (confirm('Are you sure you want to delete this portfolio?')) {
      try {
        await portfolioStore.deletePortfolio(id);
      } catch (error) {
        console.error('Error deleting portfolio:', error);
      }
    }
  }

  function formatDate(dateString: string) {
    return new Date(dateString).toLocaleString();
  }
</script>

<div class="portfolio-manager">
  <div class="header">
    <h2>Portfolio Management</h2>
    <button on:click={handleAddPortfolioClick} class="btn btn-primary">
      Add New Portfolio
    </button>
  </div>

  <!-- Single Portfolio Table with Options -->
  <div class="portfolio-table-container">
    <table class="table">
      <thead>
        <tr>
          <th>Name</th>
          <th>Underlying</th>
          <th>Description</th>
          <th>Created</th>
          <th>Updated</th>
          <th>Options</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>
        {#each $portfolioStore.portfolios as portfolio (portfolio.id)}
          <tr class:selected={selectedPortfolioId === portfolio.id}>
            <td>
              <button
                type="button"
                class="portfolio-link"
                on:click={() => selectPortfolio(portfolio.id)}
              >
                {portfolio.name}
              </button>
            </td>
            <td>{portfolio.underlying || 'N/A'}</td>
            <td>{portfolio.description || 'No description'}</td>
            <td>{formatDate(portfolio.created_at)}</td>
            <td>{formatDate(portfolio.updated_at)}</td>
            <td>
              <span class="badge">{portfolio.options?.length || 0}</span>
              {#if portfolio.options?.length > 0}
                <button 
                  class="btn btn-sm btn-outline-primary ml-2"
                  on:click|stopPropagation={() => handleAddOptionClick(portfolio)}
                >
                  Add Option
                </button>
              {/if}
            </td>
            <td class="actions">
              <button
                on:click|stopPropagation={() => handleEditPortfolio(portfolio)}
                class="btn btn-sm btn-edit"
              >
                Edit
              </button>
              <button
                on:click|stopPropagation={() => handleDelete(portfolio.id)}
                class="btn btn-sm btn-delete"
              >
                Delete
              </button>
            </td>
          </tr>

          <!-- Options Panel for Selected Portfolio -->
          {#if selectedPortfolioId === portfolio.id && portfolio.options?.length > 0}
            <tr class="options-panel">
              <td colspan="6">
                <div class="options-container">
                  <div class="options-header">
                    <h4>Options in {portfolio.name}</h4>
                    <button
                      on:click={() => handleAddOptionClick(portfolio)}
                      class="btn btn-sm btn-primary"
                    >
                      Add Option
                    </button>
                  </div>
                  <table class="options-table">
                    <thead>
                      <tr>
                        <th>Symbol</th>
                        <th>Type</th>
                        <th>Strike</th>
                        <th>Expiration</th>
                        <th>Quantity</th>
                        <th>Premium</th>
                        <th>Value</th>
                      </tr>
                    </thead>
                    <tbody>
                      {#each portfolio.options as option (option.id)}
                        <tr>
                          <td>{option.symbol}</td>
                          <td>{option.option_type?.toUpperCase() || 'N/A'}</td>
                          <td>${option.strike?.toFixed(2) || '0.00'}</td>
                          <td>{option.expiration ? new Date(option.expiration).toLocaleDateString() : 'N/A'}</td>
                          <td>{option.quantity || 0}</td>
                          <td>${option.premium?.toFixed(2) || '0.00'}</td>
                          <td>
                            ${((option.premium || 0) * (option.quantity || 0)).toFixed(2)}
                          </td>
                        </tr>
                      {/each}
                    </tbody>
                  </table>
                </div>
              </td>
            </tr>
          {:else if selectedPortfolioId === portfolio.id}
            <tr class="options-panel">
              <td colspan="6">
                <div class="no-options">
                  <p>No options found in this portfolio.</p>
                  <button
                    on:click={() => handleAddOptionClick(portfolio)}
                    class="btn btn-sm btn-primary"
                  >
                    Add Option
                  </button>
                </div>
              </td>
            </tr>
          {/if}
        {/each}
      </tbody>
    </table>
  </div>

  <!-- Portfolio Form Modal -->
  {#if showPortfolioForm}
    <div class="modal-overlay">
      <div class="modal">
        <div class="modal-content">
          <h3>{isEditingPortfolio ? 'Edit' : 'Add New'} Portfolio</h3>
          <form on:submit|preventDefault={handlePortfolioSubmit}>
            <div class="form-group">
              <label for="portfolio-name">Name</label>
              <input
                id="portfolio-name"
                type="text"
                bind:value={portfolioFormData.name}
                required
                placeholder="Enter portfolio name"
                class="form-control"
              />
            </div>
            <div class="form-group">
              <label for="portfolio-underlying">Underlying Asset</label>
              <select
                id="portfolio-underlying"
                bind:value={portfolioFormData.underlying}
                required
                class="form-control"
              >
                <option value="BTC-USD">BTC-USD</option>
                <option value="ETH-USD">ETH-USD</option>
                <option value="SOL-USD">SOL-USD</option>
                <option value="OTHER">Other</option>
              </select>
              {#if portfolioFormData.underlying === 'OTHER'}
                <input
                  type="text"
                  bind:value={portfolioFormData.underlying}
                  placeholder="Enter underlying symbol"
                  class="form-control mt-2"
                />
              {/if}
            </div>
            <div class="form-group">
              <label for="portfolio-description">Description (Optional)</label>
              <textarea
                id="portfolio-description"
                bind:value={portfolioFormData.description}
                placeholder="Enter description"
                class="form-control"
                rows="3"
              ></textarea>
            </div>
            <div class="form-actions">
              <button type="button" class="btn btn-secondary" on:click={() => showPortfolioForm = false}>
                Cancel
              </button>
              <button type="submit" class="btn btn-primary">
                {isEditingPortfolio ? 'Update' : 'Create'} Portfolio
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  {/if}

  <!-- Option Form -->
  {#if showOptionForm}
    <div class="modal-overlay">
      <div class="modal">
        <div class="modal-content">
          <h3>Add New Option</h3>
          <form on:submit|preventDefault={handleOptionSubmit} class="option-form">
            <div class="form-row">
              <div class="form-group">
                <label for="underlying">Underlying</label>
                <input
                  type="text"
                  id="underlying"
                  bind:value={optionFormData.underlying}
                  class="form-control"
                  readonly
                />
              </div>

              <div class="form-group">
                <label for="expiry">Expiry</label>
                <input
                  type="date"
                  id="expiry"
                  bind:value={optionFormData.expiry}
                  class="form-control"
                  required
                />
              </div>
            </div>

            <div class="form-row">
              <div class="form-group">
                <label for="strike">Strike</label>
                <input
                  type="number"
                  id="strike"
                  bind:value={optionFormData.strike}
                  class="form-control"
                  min="0"
                  step="0.01"
                  required
                />
              </div>

              <div class="form-group">
                <label for="optionType">Option Type</label>
                <select
                  id="optionType"
                  bind:value={optionFormData.optionType}
                  class="form-control"
                >
                  <option value="call">Call</option>
                  <option value="put">Put</option>
                </select>
              </div>
            </div>


            <div class="form-row">
              <div class="form-group">
                <label for="contractType">Contract Type</label>
                <select
                  id="contractType"
                  bind:value={optionFormData.contractType}
                  class="form-control"
                >
                  <option value="STANDARD">Standard</option>
                  <option value="INVERSE">Inverse</option>
                </select>
              </div>

              <div class="form-group">
                <label for="quantity">Quantity</label>
                <input
                  type="number"
                  id="quantity"
                  bind:value={optionFormData.quantity}
                  class="form-control"
                  min="1"
                  step="1"
                  required
                />
              </div>
            </div>


            <div class="form-group">
              <label for="premium">Premium ({optionFormData.contractType === 'INVERSE' ? optionFormData.underlying : 'USD'})</label>
              <input
                type="number"
                id="premium"
                bind:value={optionFormData.premium}
                class="form-control"
                min="0"
                step="0.00000001"
                required
              />
            </div>

            <div class="form-group">
              <label for="instrument-name-preview">Instrument Name</label>
              <div id="instrument-name-preview" class="instrument-name-preview" role="status" aria-live="polite">
                {optionFormData.instrumentName || 'Fill in the fields above'}
              </div>
            </div>

            <div class="form-actions">
              <button type="button" on:click={() => showOptionForm = false} class="btn btn-secondary">
                Cancel
              </button>
              <button type="submit" class="btn btn-primary">
                Save Option
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  {/if}
</div>

<style>
  .portfolio-manager {
    max-width: 1400px;
    margin: 0 auto;
    padding: 20px;
  }

  .portfolio-table-container {
    margin-top: 20px;
    background: white;
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    overflow: hidden;
  }

  .table {
    width: 100%;
    border-collapse: collapse;
  }

  .table th,
  .table td {
    padding: 12px 15px;
    text-align: left;
    border-bottom: 1px solid #eee;
  }

  .table th {
    background-color: #f8f9fa;
    font-weight: 600;
    color: #495057;
  }

  .table tbody tr:hover {
    background-color: #f8f9fa;
  }

  .table tbody tr.selected {
    background-color: #e9ecef;
  }

  button.portfolio-link {
    background: none;
    border: none;
    color: #007bff;
    cursor: pointer;
    font: inherit;
    padding: 0;
    text-decoration: underline;
  }

  button.portfolio-link:hover {
    text-decoration: none;
  }

  .actions {
    display: flex;
    gap: 8px;
  }

  .btn {
    padding: 6px 12px;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    font-size: 14px;
    transition: all 0.2s;
  }

  .btn-sm {
    padding: 4px 8px;
    font-size: 12px;
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

  .btn-edit {
    background-color: #17a2b8;
    color: white;
  }

  .btn-edit:hover {
    background-color: #138496;
  }

  .btn-delete {
    background-color: #dc3545;
    color: white;
  }

  .btn-delete:hover {
    background-color: #c82333;
  }

  /* Options Panel Styles */
  .options-panel {
    background-color: #f8f9fa;
  }

  .options-container {
    padding: 0;
  }

  .options-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px 15px;
    background-color: #f8f9fa;
    border-bottom: 1px solid #dee2e6;
  }

  .options-header h4 {
    margin: 0;
    color: #495057;
    font-size: 16px;
    font-weight: 600;
  }

  .options-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 14px;
  }

  .options-table th,
  .options-table td {
    padding: 8px 12px;
    border: 1px solid #dee2e6;
  }

  .options-table th {
    background-color: #f8f9fa;
    font-weight: 600;
    color: #495057;
  }

  .options-table tbody tr {
    background-color: #ffffff;
  }

  .options-table tbody tr:hover {
    background-color: #f1f3f5;
  }

  .no-options {
    padding: 20px;
    text-align: center;
    color: #6c757d;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 15px;
  }

  .no-options p {
    margin: 0;
    font-style: italic;
  }

  /* Form Styles */
  /* Modal Styles */
  .modal-overlay {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background-color: rgba(0, 0, 0, 0.5);
    display: flex;
    justify-content: center;
    align-items: center;
    z-index: 1000;
  }

  .modal {
    background: white;
    border-radius: 8px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
    width: 100%;
    max-width: 600px;
    max-height: 90vh;
    overflow-y: auto;
  }

  .modal-content {
    padding: 24px;
  }

  .modal h3 {
    margin-top: 0;
    margin-bottom: 20px;
    color: #2c3e50;
    font-size: 1.5rem;
  }

  /* Modal Form Styles */

  .form-group {
    margin-bottom: 15px;
  }

  .form-group label {
    display: block;
    margin-bottom: 5px;
    font-weight: 500;
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
    margin-top: 20px;
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

  .form-group {
    margin-bottom: 15px;
  }

  label {
    display: block;
    margin-bottom: 5px;
    font-weight: 500;
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
    margin-top: 20px;
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

  .btn-edit {
    background-color: #ffc107;
    color: #212529;
  }

  .btn-delete {
    background-color: #dc3545;
    color: white;
  }

  .table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 15px;
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


</style>
