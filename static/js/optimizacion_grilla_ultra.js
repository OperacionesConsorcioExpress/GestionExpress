/**
 * ULTRA GRID OPTIMIZER - Sistema avanzado de optimización de tablas
 * Implementa virtualización DOM, pooling de elementos y rendering batch
 * Objetivo: Renderizar 5000+ registros en <100ms
 * Autor: Sistema de Optimización SGI
 * Fecha: Noviembre 2025
 */

class UltraGridOptimizer {
    constructor(config = {}) {
        this.config = {
            // Configuración de virtualización
            visibleRows: config.visibleRows || 50,
            bufferRows: config.bufferRows || 10,
            rowHeight: config.rowHeight || 40,
            
            // Pool de elementos DOM
            maxPoolSize: config.maxPoolSize || 100,
            
            // Performance
            scrollThrottleMs: config.scrollThrottleMs || 16, // 60fps
            renderBatchSize: config.renderBatchSize || 25,
            
            // Callbacks
            onRowRender: config.onRowRender || null,
            onPerformanceMetric: config.onPerformanceMetric || null,
            
            // Debug
            debug: config.debug || false
        };

        // Estado interno
        this.data = [];
        this.filteredData = [];
        this.scrollTop = 0;
        this.containerHeight = 0;
        this.totalHeight = 0;
        
        // DOM Elements Pool
        this.rowPool = [];
        this.activeRows = new Map();
        
        // Performance tracking
        this.metrics = {
            lastRenderTime: 0,
            totalRenders: 0,
            averageRenderTime: 0,
            cacheHits: 0,
            domReuses: 0
        };

        // Throttled functions
        this.throttledScroll = this.throttle(this.handleScroll.bind(this), this.config.scrollThrottleMs);
        
        // Cache de renderizado
        this.renderCache = new Map();
        
        this.log('🚀 Ultra Grid Optimizer inicializado', this.config);
    }

    /**
     * Inicializa el optimizador en un contenedor DOM
     */
    initialize(container, tableElement) {
        this.container = typeof container === 'string' ? document.getElementById(container) : container;
        this.table = typeof tableElement === 'string' ? document.getElementById(tableElement) : tableElement;
        
        if (!this.container || !this.table) {
            throw new Error('Container o tabla no encontrados');
        }

        this.tbody = this.table.querySelector('tbody');
        if (!this.tbody) {
            this.tbody = document.createElement('tbody');
            this.table.appendChild(this.tbody);
        }

        // Configurar estilos de virtualización
        this.setupVirtualization();
        
        // Event listeners
        this.container.addEventListener('scroll', this.throttledScroll);
        window.addEventListener('resize', this.throttle(this.handleResize.bind(this), 100));

        this.log('✅ Grid optimizer inicializado en DOM');
        return this;
    }

    /**
     * Configura la virtualización DOM
     */
    setupVirtualization() {
        // Wrapper para scroll virtual
        this.virtualWrapper = document.createElement('div');
        this.virtualWrapper.style.cssText = `
            position: relative;
            overflow: auto;
            height: 100%;
        `;

        // Contenedor de altura total (spacer)
        this.spacer = document.createElement('div');
        this.spacer.style.cssText = `
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            pointer-events: none;
        `;

        // Viewport para filas visibles
        this.viewport = document.createElement('div');
        this.viewport.style.cssText = `
            position: relative;
            transform: translateY(0px);
        `;

        // Reestructurar DOM
        const parent = this.container.parentNode;
        parent.insertBefore(this.virtualWrapper, this.container);
        this.virtualWrapper.appendChild(this.spacer);
        this.virtualWrapper.appendChild(this.viewport);
        this.viewport.appendChild(this.container);

        this.containerHeight = this.virtualWrapper.clientHeight;
    }

    /**
     * Carga datos en el grid optimizado
     */
    loadData(data, filters = {}) {
        const startTime = performance.now();
        
        this.data = Array.isArray(data) ? data : [];
        this.applyFilters(filters);
        this.calculateDimensions();
        this.clearActiveRows();
        this.renderVisibleRows();
        
        const renderTime = performance.now() - startTime;
        this.updateMetrics('loadData', renderTime);
        
        this.log(`📊 Datos cargados: ${this.data.length} registros en ${renderTime.toFixed(2)}ms`);
        
        // Callback de performance
        if (this.config.onPerformanceMetric) {
            this.config.onPerformanceMetric({
                operation: 'loadData',
                recordCount: this.data.length,
                renderTime: renderTime,
                fps: 1000 / renderTime
            });
        }
        
        return this;
    }

    /**
     * Aplica filtros a los datos
     */
    applyFilters(filters) {
        if (!filters || Object.keys(filters).length === 0) {
            this.filteredData = [...this.data];
            return;
        }

        this.filteredData = this.data.filter(row => {
            return Object.entries(filters).every(([key, value]) => {
                if (!value || value === '') return true;
                
                const rowValue = row[key];
                if (typeof rowValue === 'string') {
                    return rowValue.toLowerCase().includes(value.toLowerCase());
                }
                return rowValue === value;
            });
        });
    }

    /**
     * Calcula dimensiones para virtualización
     */
    calculateDimensions() {
        this.totalHeight = this.filteredData.length * this.config.rowHeight;
        this.spacer.style.height = `${this.totalHeight}px`;
        
        this.maxVisibleRows = Math.ceil(this.containerHeight / this.config.rowHeight) + this.config.bufferRows;
    }

    /**
     * Renderiza solo las filas visibles
     */
    renderVisibleRows() {
        const startTime = performance.now();
        
        const scrollTop = this.virtualWrapper.scrollTop;
        const startIndex = Math.floor(scrollTop / this.config.rowHeight);
        const endIndex = Math.min(
            startIndex + this.maxVisibleRows,
            this.filteredData.length
        );

        // Limpiar filas no visibles
        this.clearInvisibleRows(startIndex, endIndex);

        // Renderizar en batches para mejor performance
        this.renderBatch(startIndex, endIndex);

        // Actualizar posición del viewport
        this.viewport.style.transform = `translateY(${startIndex * this.config.rowHeight}px)`;

        const renderTime = performance.now() - startTime;
        this.updateMetrics('renderVisibleRows', renderTime);
        
        this.log(`🖼️ Renderizadas filas ${startIndex}-${endIndex} en ${renderTime.toFixed(2)}ms`);
    }

    /**
     * Renderiza filas en batches para mejor performance
     */
    renderBatch(startIndex, endIndex) {
        const fragment = document.createDocumentFragment();
        let batchCount = 0;

        for (let i = startIndex; i < endIndex; i++) {
            if (this.activeRows.has(i)) continue;

            const row = this.createOrReuseRow(this.filteredData[i], i);
            if (row) {
                fragment.appendChild(row);
                this.activeRows.set(i, row);
                batchCount++;

                // Batch rendering para evitar bloquear UI
                if (batchCount >= this.config.renderBatchSize) {
                    this.tbody.appendChild(fragment);
                    batchCount = 0;
                    // Yield control back to browser
                    setTimeout(() => this.renderBatch(i + 1, endIndex), 0);
                    return;
                }
            }
        }

        if (fragment.children.length > 0) {
            this.tbody.appendChild(fragment);
        }
    }

    /**
     * Crea o reutiliza una fila del pool
     */
    createOrReuseRow(data, index) {
        let row;

        // Intentar reutilizar del pool
        if (this.rowPool.length > 0) {
            row = this.rowPool.pop();
            this.metrics.domReuses++;
        } else {
            row = document.createElement('tr');
        }

        // Verificar cache de renderizado
        const cacheKey = this.getCacheKey(data, index);
        if (this.renderCache.has(cacheKey)) {
            const cachedContent = this.renderCache.get(cacheKey);
            row.innerHTML = cachedContent;
            this.metrics.cacheHits++;
        } else {
            // Renderizar nueva fila
            this.populateRow(row, data, index);
            this.renderCache.set(cacheKey, row.innerHTML);
        }

        row.dataset.index = index;
        row.style.height = `${this.config.rowHeight}px`;

        return row;
    }

    /**
     * Popula una fila con datos
     */
    populateRow(row, data, index) {
        if (this.config.onRowRender) {
            this.config.onRowRender(row, data, index);
        } else {
            // Renderizado por defecto
            row.innerHTML = Object.values(data).map(value => 
                `<td>${this.escapeHtml(value || '')}</td>`
            ).join('');
        }
    }

    /**
     * Limpia filas no visibles y las devuelve al pool
     */
    clearInvisibleRows(visibleStart, visibleEnd) {
        const toRemove = [];

        this.activeRows.forEach((row, index) => {
            if (index < visibleStart || index >= visibleEnd) {
                toRemove.push(index);
            }
        });

        toRemove.forEach(index => {
            const row = this.activeRows.get(index);
            if (row && row.parentNode) {
                row.parentNode.removeChild(row);
                
                // Devolver al pool si no está lleno
                if (this.rowPool.length < this.config.maxPoolSize) {
                    this.rowPool.push(row);
                }
            }
            this.activeRows.delete(index);
        });
    }

    /**
     * Limpia todas las filas activas
     */
    clearActiveRows() {
        this.activeRows.forEach(row => {
            if (row.parentNode) {
                row.parentNode.removeChild(row);
            }
            if (this.rowPool.length < this.config.maxPoolSize) {
                this.rowPool.push(row);
            }
        });
        this.activeRows.clear();
    }

    /**
     * Maneja scroll event
     */
    handleScroll() {
        this.scrollTop = this.virtualWrapper.scrollTop;
        this.renderVisibleRows();
    }

    /**
     * Maneja resize event
     */
    handleResize() {
        this.containerHeight = this.virtualWrapper.clientHeight;
        this.calculateDimensions();
        this.renderVisibleRows();
    }

    /**
     * Actualiza filtros dinámicamente
     */
    updateFilters(filters) {
        const startTime = performance.now();
        
        this.applyFilters(filters);
        this.calculateDimensions();
        this.clearActiveRows();
        this.renderVisibleRows();
        
        const renderTime = performance.now() - startTime;
        this.updateMetrics('updateFilters', renderTime);
        
        this.log(`🔍 Filtros actualizados: ${this.filteredData.length} registros en ${renderTime.toFixed(2)}ms`);
    }

    /**
     * Actualiza métricas de performance
     */
    updateMetrics(operation, time) {
        this.metrics.lastRenderTime = time;
        this.metrics.totalRenders++;
        this.metrics.averageRenderTime = (
            (this.metrics.averageRenderTime * (this.metrics.totalRenders - 1)) + time
        ) / this.metrics.totalRenders;
    }

    /**
     * Obtiene métricas de performance
     */
    getMetrics() {
        return {
            ...this.metrics,
            dataCount: this.data.length,
            filteredCount: this.filteredData.length,
            activeRowsCount: this.activeRows.size,
            poolSize: this.rowPool.length,
            cacheSize: this.renderCache.size
        };
    }

    /**
     * Utilities
     */
    getCacheKey(data, index) {
        return `${index}_${JSON.stringify(data).slice(0, 50)}`;
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    throttle(func, delay) {
        let timeoutId;
        let lastExecTime = 0;
        return function (...args) {
            const currentTime = Date.now();
            
            if (currentTime - lastExecTime > delay) {
                func.apply(this, args);
                lastExecTime = currentTime;
            } else {
                clearTimeout(timeoutId);
                timeoutId = setTimeout(() => {
                    func.apply(this, args);
                    lastExecTime = Date.now();
                }, delay - (currentTime - lastExecTime));
            }
        };
    }

    log(message, data = null) {
        if (this.config.debug) {
            console.log(`[UltraGridOptimizer] ${message}`, data || '');
        }
    }

    /**
     * Destructor - limpia recursos
     */
    destroy() {
        this.container?.removeEventListener('scroll', this.throttledScroll);
        window.removeEventListener('resize', this.handleResize);
        
        this.clearActiveRows();
        this.rowPool = [];
        this.renderCache.clear();
        
        this.log('🗑️ Ultra Grid Optimizer destruido');
    }
}

// Export para uso global
window.UltraGridOptimizer = UltraGridOptimizer;

// Auto-inicialización si se encuentra el elemento
document.addEventListener('DOMContentLoaded', function() {
    const autoInitElement = document.querySelector('[data-ultra-grid]');
    if (autoInitElement) {
        const config = JSON.parse(autoInitElement.dataset.ultraGrid || '{}');
        const optimizer = new UltraGridOptimizer(config);
        optimizer.initialize(autoInitElement.dataset.container, autoInitElement);
        
        // Exponer globalmente para debugging
        window.ultraGridOptimizer = optimizer;
    }
});
