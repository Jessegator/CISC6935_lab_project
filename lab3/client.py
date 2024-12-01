from xmlrpc.client import ServerProxy

class Client:
    def __init__(self):
        # Modify the ip address and port number of the coordinator correspondingly
        self.coordinator = ServerProxy("http://10.128.0.09:8001", allow_none=True)
    
    def reset_for_scenario(self, scenario,crash_scenario=None):
        """Reset balances for different test scenarios"""
        print(f"\nResetting for scenario {scenario.upper()}")
        self.coordinator.reset_scenario(scenario,crash_scenario)
        print("Initial balances:")
        self.print_balances()
        print("-" * 50)

    def print_balances(self):
        """Print current account balances"""
        try:
            balances = self.coordinator.get_balances()
            print(f"Account A balance: {balances['2']}")  # Use string keys
            print(f"Account B balance: {balances['3']}")
        except Exception as e:
            print(f"Error getting balances: {e}")
        
    def execute_transaction(self, transaction, crash_scenario=None):
        """Execute a single transaction with error handling"""
        print(f"\nExecuting transaction: {transaction}")
        if crash_scenario:
            print(f"With crash scenario: {crash_scenario}")
        
        try:
            result = self.coordinator.execute_transaction(transaction, crash_scenario)
            print(f"Transaction result: {result['message']}")
            print("Current balances:")
            self.print_balances()
            return result['status'] == 'success'
        except Exception as e:
            print(f"Error during transaction: {e}")
            return False
            
    def run_scenario_transactions(self, scenario, crash_scenario=None):
        """Execute both transactions consecutively"""
        print(f"\n=== Running Scenario {scenario.upper()} ===")
        self.reset_for_scenario(scenario, crash_scenario)
        
        # Transaction 1: Transfer 100 from A to B
        print("\nTransaction 1: Transfer 100 from A to B")
        transfer_result = self.execute_transaction({
            'type': 'transfer',
            'amount': 100
        }, crash_scenario)
        
        if not transfer_result:
            print("\nWARNING: Transfer transaction failed")
        
        # Transaction 2: Add 20% bonus to both accounts
        # Always attempt bonus transaction even if transfer failed
        print("\nTransaction 2: Add 20% bonus to both accounts")
        bonus_result = self.execute_transaction({
            'type': 'bonus'
        }, crash_scenario)
        
        if not bonus_result:
            print("\nWARNING: Bonus transaction failed")
        
        print("\nScenario completed")
        print("Final balances:")
        self.print_balances()
        print("-" * 50)

def print_menu():
    """Print main menu"""
    print("\n=== 2PC Transaction Simulator ===")
    print("1. Scenario A: Normal operation (A=200, B=300)")
    print("2. Scenario B: Insufficient funds (A=90, B=50)")
    print("3. Scenario C: Node crash simulation (A=200, B=300)")
    print("4. Print current balances")
    print("5. Exit")

def handle_scenario_c():
    """Handle crash scenario selection"""
    print("\nScenario C options:")
    print("1. Node-2 crashes before responding")
    print("2. Node-2 crashes after responding")
    print("3. Coordinator crashes after sending request")
    return input("Choose crash scenario (1-3): ")

def main():
    client = Client()
    
    while True:
        print_menu()
        choice = input("\nEnter your choice (1-5): ")
        
        if choice == '1':
            client.run_scenario_transactions('a')
        elif choice == '2':
            client.run_scenario_transactions('b')
        elif choice == '3':
            crash_choice = handle_scenario_c()
            crash_scenario = None
            
            if crash_choice == '1':
                crash_scenario = 'c1' # Node 2 crash before responding
            elif crash_choice == '2':
                crash_scenario = 'c2' # Node 2 crash after responding
            elif crash_choice == '3':
                crash_scenario = 'c3'
                
            client.run_scenario_transactions('c', crash_scenario)
        elif choice == '4':
            client.print_balances()
        elif choice == '5':
            print("Exiting simulator...")
            break
        else:
            print("Invalid choice! Please try again.")

if __name__ == "__main__":
    main()