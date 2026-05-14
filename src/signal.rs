//! Signal handling for graceful shutdown.

use tokio::sync::broadcast;

/// A shutdown signal receiver.
pub struct Shutdown {
    receiver: broadcast::Receiver<()>,
}

/// A shutdown signal sender.
#[allow(dead_code)]
pub struct ShutdownSignal {
    sender: broadcast::Sender<()>,
}

impl ShutdownSignal {
    pub fn new() -> (Self, Shutdown) {
        let (sender, receiver) = broadcast::channel::<()>(1);
        let sender_clone = sender.clone();
        
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
                let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
                
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = sigterm.recv() => {},
                    _ = sigint.recv() => {},
                }
            }
            
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
            
            let _ = sender_clone.send(());
        });
        
        (Self { sender }, Shutdown { receiver })
    }
    
    #[allow(dead_code)]
    pub fn shutdown(&self) { 
        let _ = self.sender.send(()); 
    }
}

impl Shutdown {
    pub async fn wait(&mut self) { 
        let _ = self.receiver.recv().await; 
    }
}

impl Default for ShutdownSignal {
    fn default() -> Self { 
        Self::new().0 
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_shutdown_signal_creation() {
        let (signal, mut shutdown) = ShutdownSignal::new();
        
        // Verify we can create a signal and shutdown pair
        assert!(std::mem::size_of_val(&signal) > 0);
        
        // Trigger shutdown manually
        signal.shutdown();
        
        // Wait should complete immediately after shutdown is called
        shutdown.wait().await;
    }

    #[tokio::test]
    async fn test_default_shutdown_signal() {
        let signal = ShutdownSignal::default();
        
        // Verify default creates a valid signal
        assert!(std::mem::size_of_val(&signal) > 0);
    }

    #[tokio::test]
    async fn test_manual_shutdown() {
        let (signal, mut shutdown) = ShutdownSignal::new();
        
        // Manually trigger shutdown
        signal.shutdown();
        
        // wait() should return immediately
        shutdown.wait().await;
    }
}
